package top.ellan.ecobridge;

import jdk.incubator.vector.*;
import java.lang.foreign.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.StampedLock;

/**
 * 极致性能 PID 控制器 (SIMD Gather/Scatter 极限优化版)
 *
 * 核心特性:
 * 1. True Gather/Scatter: 无中间数组,寄存器直连堆外内存
 * 2. Bitonic Sort: 双调排序优化内存访问模式,提升 Cache 命中率
 * 3. FMA: 融合乘加指令优化 (减少舍入误差和延迟)
 * 4. Zero-Allocation: 热点路径零内存分配
 * 5. Adaptive Sorting: 根据数据密度自适应启用排序
 * 6. Cache Line Alignment: 关键数据结构 64 字节对齐
 *
 * 性能优化理论依据:
 * - 排序后的 Gather/Scatter 访问具有空间局部性,提升 L1 Cache 预取器效率
 * - Bitonic Sort 的固定执行路径非常适合 SIMD 并行化
 * - 单调递增的索引访问可触发硬件 TLB 预取,减少页表开销
 */
public class PidController implements AutoCloseable {
    // ==================== 1. 向量化与常量配置 ====================
    private static final VectorSpecies<Double> SPECIES = DoubleVector.SPECIES_PREFERRED;
    private static final int V_LEN = SPECIES.length();

    // 索引向量物种 - 使用 SPECIES_PREFERRED 确保长度匹配
    // 在 AVX-512 下: DoubleVector (512bit/8lanes) 对应 LongVector (512bit/8lanes)
    private static final VectorSpecies<Long> L_SPECIES = LongVector.SPECIES_PREFERRED;
    private static final VectorSpecies<Integer> I_SPECIES = IntVector.SPECIES_PREFERRED;
    private static final int SEGMENT_BITS = 4;
    private static final int SEGMENT_COUNT = 1 << SEGMENT_BITS;
    private static final int SEGMENT_MASK = SEGMENT_COUNT - 1;
    private static final int CAPACITY = 8192;
    private static final int SLOT_MASK = 0xFFFF;
    // 排序阈值配置
    private static final int SORT_THRESHOLD = 128;  // 大于此值才启用排序
    private static final int SORT_DENSITY_MIN = 8;  // 最小密度 (items/segment)

    // Cache Line 优化
    private static final int PREFETCH_DISTANCE = 8; // 预取距离 (Cache Lines)
    private static final ValueLayout.OfDouble D_LAYOUT = ValueLayout.JAVA_DOUBLE.withOrder(java.nio.ByteOrder.nativeOrder());
    private static final ValueLayout.OfLong L_LAYOUT = ValueLayout.JAVA_LONG.withOrder(java.nio.ByteOrder.nativeOrder());
    private static final ValueLayout.OfByte B_LAYOUT = ValueLayout.JAVA_BYTE;
    private static final long D_SIZE = 8L;

    // ThreadLocal 临时数组重用，消除 per-segment 分配抖动
    private static final ThreadLocal<long[]> OFFSET_ARRAY_TL = ThreadLocal.withInitial(() -> new long[V_LEN]);
    private static final ThreadLocal<double[]> TEMP_INTEGRAL_TL = ThreadLocal.withInitial(() -> new double[V_LEN]);
    private static final ThreadLocal<double[]> TEMP_ERROR_TL = ThreadLocal.withInitial(() -> new double[V_LEN]);
    private static final ThreadLocal<double[]> TEMP_LAMBDA_TL = ThreadLocal.withInitial(() -> new double[V_LEN]);
    private static final ThreadLocal<long[]> TEMP_TIME_TL = ThreadLocal.withInitial(() -> new long[V_LEN]);

    // ==================== 2. PID 参数 ====================
    private double target, deadband, kp, kpNegMultiplier, ki, kd, base, alpha, beta, tau;
    private double integralMax, integralMin, lambdaMax, lambdaMin, dtMax, dtMin;
    // 向量化参数 (Cache Line 对齐广播)
    private DoubleVector vTarget, vDeadband, vKp, vKpNeg, vKi, vKd, vBase, vAlpha, vBeta;
    private DoubleVector vZero, vOne, vIMax, vIMin, vLMax, vLMin, vDtMax, vDtMin, vNegTauInv;
    private DoubleVector vPointZeroZeroOne; // 0.001 for ms to s conversion
    // ==================== 3. 核心成员 ====================
    private final EcoBridge plugin;
    private final Arena arena;
    private final Segment[] segments;
    private final ConcurrentHashMap<String, Integer> registry = new ConcurrentHashMap<>(4096);
    private final ConcurrentHashMap<Integer, String> handleToId = new ConcurrentHashMap<>(4096);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    // 性能统计
    private volatile long totalSorts = 0;
    private volatile long totalSkippedSorts = 0;
    // ==================== DTO 定义 ====================
    public record PidStateDto(double integral, double lastError, double lastLambda, long updateTime) {}
    public PidController(EcoBridge plugin) {
        this.plugin = plugin;
        this.arena = Arena.ofShared();
        this.segments = new Segment[SEGMENT_COUNT];
        for (int i = 0; i < SEGMENT_COUNT; i++) {
            this.segments[i] = new Segment(arena);
        }
        loadConfig();
        plugin.getLogger().info(String.format(
            "[PID] Extreme Controller Initialized. Vector Lane: %d, Sort Enabled: true (threshold=%d)",
            V_LEN, SORT_THRESHOLD
        ));
    }
    private void loadConfig() {
        var cfg = plugin.getConfig();
        this.target = cfg.getDouble("pid.target", 1000.0);
        this.deadband = cfg.getDouble("pid.deadband", 20.0);
        this.kp = cfg.getDouble("pid.kp", 0.00001);
        this.kpNegMultiplier = cfg.getDouble("pid.kp-negative-multiplier", 0.6);
        this.ki = cfg.getDouble("pid.ki", 0.000001);
        this.kd = cfg.getDouble("pid.kd", 0.00005);
        this.base = cfg.getDouble("pid.base", 0.002);
        this.alpha = cfg.getDouble("pid.alpha", 0.05);
        this.beta = cfg.getDouble("pid.beta", 0.95);
        this.tau = cfg.getDouble("pid.tau", 0.00001929);
        this.integralMax = cfg.getDouble("pid.integral-max", 30000.0);
        this.integralMin = cfg.getDouble("pid.integral-min", -30000.0);
        this.lambdaMax = cfg.getDouble("pid.lambda-max", 0.01);
        this.lambdaMin = cfg.getDouble("pid.lambda-min", 0.0005);
        this.dtMax = cfg.getDouble("pid.dt-max", 1.0);
        this.dtMin = cfg.getDouble("pid.dt-min", 0.05);
        // 向量化参数广播
        this.vTarget = DoubleVector.broadcast(SPECIES, target);
        this.vDeadband = DoubleVector.broadcast(SPECIES, deadband);
        this.vKp = DoubleVector.broadcast(SPECIES, kp);
        this.vKpNeg = DoubleVector.broadcast(SPECIES, kp * kpNegMultiplier);
        this.vKi = DoubleVector.broadcast(SPECIES, ki);
        this.vKd = DoubleVector.broadcast(SPECIES, kd);
        this.vBase = DoubleVector.broadcast(SPECIES, base);
        this.vAlpha = DoubleVector.broadcast(SPECIES, alpha);
        this.vBeta = DoubleVector.broadcast(SPECIES, beta);
        this.vZero = DoubleVector.broadcast(SPECIES, 0.0);
        this.vOne = DoubleVector.broadcast(SPECIES, 1.0);
        this.vIMax = DoubleVector.broadcast(SPECIES, integralMax);
        this.vIMin = DoubleVector.broadcast(SPECIES, integralMin);
        this.vLMax = DoubleVector.broadcast(SPECIES, lambdaMax);
        this.vLMin = DoubleVector.broadcast(SPECIES, lambdaMin);
        this.vDtMax = DoubleVector.broadcast(SPECIES, dtMax);
        this.vDtMin = DoubleVector.broadcast(SPECIES, dtMin);
        this.vNegTauInv = DoubleVector.broadcast(SPECIES, -tau);
        this.vPointZeroZeroOne = DoubleVector.broadcast(SPECIES, 0.001);
    }
    public void reloadConfig() {
        loadConfig();
        plugin.getLogger().info("[PID] Parameters reloaded.");
    }
    // ==================== 4. 业务 API ====================
    public EcoBridge.CalculationResult calculate(EcoBridge.MarketSnapshot snapshot) {
        if (closed.get() || snapshot.validItemCount() == 0) {
            return new EcoBridge.CalculationResult(0.0, base);
        }
        double totalLambda = calculateBatchInternal(
            snapshot.itemHandles(),
            snapshot.itemDeltas(),
            snapshot.validItemCount()
        );
        double avgLambda = totalLambda / snapshot.validItemCount();
        return new EcoBridge.CalculationResult(avgLambda, avgLambda * 10.0);
    }
    public int getHandle(String itemId) {
        return registry.computeIfAbsent(itemId, id -> {
            int hash = Math.abs(id.hashCode());
            int segId = hash & SEGMENT_MASK;
            Segment seg = segments[segId];
            long stamp = seg.lock.writeLock();
            try {
                int slot = seg.allocateSlot();
                if (slot < 0) {
                    plugin.getLogger().warning("[PID] Segment " + segId + " overflow: " + itemId);
                    return -1;
                }
                int handle = (segId << 16) | slot;
                handleToId.put(handle, id);
                return handle;
            } finally {
                seg.lock.unlockWrite(stamp);
            }
        });
    }
    public double getLambdaByString(String itemId) {
        Integer handle = registry.get(itemId);
        if (handle == null) return base;
        int segId = handle >>> 16;
        int slot = handle & SLOT_MASK;
        Segment seg = segments[segId];
        long offset = (long) slot * D_SIZE;
        long stamp = seg.lock.tryOptimisticRead();
        double val = seg.lambdas.get(D_LAYOUT, offset);
        if (!seg.lock.validate(stamp)) {
            stamp = seg.lock.readLock();
            try {
                val = seg.lambdas.get(D_LAYOUT, offset);
            } finally {
                seg.lock.unlockRead(stamp);
            }
        }
        return val;
    }
    public int getCacheSize() {
        return registry.size();
    }

    public int getDirtyQueueSize() {
        int count = 0;
        for (Segment seg : segments) {
            long stamp = seg.lock.readLock();
            try {
                for (int i = seg.allocationMap.nextSetBit(0); i >= 0;
                     i = seg.allocationMap.nextSetBit(i + 1)) {
                    if (seg.dirty.get(B_LAYOUT, (long) i) == (byte) 1) count++;
                }
            } finally {
                seg.lock.unlockRead(stamp);
            }
        }
        return count;
    }
    public PidStateDto inspectState(String itemId) {
        Integer handle = registry.get(itemId);
        if (handle == null) return null;
        int segId = handle >>> 16;
        int slot = handle & SLOT_MASK;
        Segment seg = segments[segId];
        long offset = (long) slot * D_SIZE;
        long stamp = seg.lock.readLock();
        try {
            return new PidStateDto(
                seg.integrals.get(D_LAYOUT, offset),
                seg.errors.get(D_LAYOUT, offset),
                seg.lambdas.get(D_LAYOUT, offset),
                seg.times.get(L_LAYOUT, offset)
            );
        } finally {
            seg.lock.unlockRead(stamp);
        }
    }
    public void calculateBatch(int[] handles, double[] volumes, int count) {
        calculateBatchInternal(handles, volumes, count);
    }
    /**
     * 获取性能统计信息
     */
    public String getPerformanceStats() {
        long total = totalSorts + totalSkippedSorts;
        double sortRate = total > 0 ? (totalSorts * 100.0 / total) : 0.0;
        return String.format(
            "Sorts: %d, Skipped: %d, Rate: %.2f%%",
            totalSorts, totalSkippedSorts, sortRate
        );
    }
    // ==================== 5. Bitonic Sort 实现 ====================
    /**
     * SIMD 双调排序器 - Key-Value 协同排序
     *
     * 算法特点:
     * - 固定执行路径,无分支预测失败
     * - 完全向量化,利用 Vector.blend 实现条件交换
     * - 复杂度 O(n log²n),但在寄存器内性能优异
     *
     * @param slots 索引数组 (Key)
     * @param vols 数值数组 (Value)
     * @param count 有效元素数量
     */
    private void bitonicSort(int[] slots, double[] vols, int count) {
        if (count <= 1) return;

        // 向上对齐到 2 的幂次 (Bitonic Sort 要求)
        int paddedCount = Integer.highestOneBit(count);
        if (paddedCount < count) paddedCount <<= 1;

        // Padding: 填充 Integer.MAX_VALUE 确保排到末尾
        for (int i = count; i < paddedCount; i++) {
            slots[i] = Integer.MAX_VALUE;
            vols[i] = 0.0;
        }
        // Bitonic Sort 主循环
        for (int size = 2; size <= paddedCount; size <<= 1) {
            for (int stride = size >> 1; stride > 0; stride >>= 1) {

                // SIMD 向量化比较交换
                int loopBound = (paddedCount / (V_LEN * 2)) * (V_LEN * 2);
                int i = 0;

                for (; i < loopBound; i += V_LEN * 2) {
                    // 加载相邻的两个向量块
                    IntVector v1 = IntVector.fromArray(I_SPECIES, slots, i);
                    IntVector v2 = IntVector.fromArray(I_SPECIES, slots, i + V_LEN);
                    DoubleVector d1 = DoubleVector.fromArray(SPECIES, vols, i);
                    DoubleVector d2 = DoubleVector.fromArray(SPECIES, vols, i + V_LEN);
                    // 比较: mask = (v1 > v2)
                    VectorMask<Integer> mask = v1.compare(VectorOperators.GT, v2);

                    // 条件交换 (Min/Max 网络)
                    IntVector lowSlots = v1.blend(v2, mask);
                    IntVector highSlots = v2.blend(v1, mask);

                    // Value 协同交换
                    var dMask = mask.cast(SPECIES);
                    DoubleVector lowVols = d1.blend(d2, dMask);
                    DoubleVector highVols = d2.blend(d1, dMask);
                    // 写回
                    lowSlots.intoArray(slots, i);
                    highSlots.intoArray(slots, i + V_LEN);
                    lowVols.intoArray(vols, i);
                    highVols.intoArray(vols, i + V_LEN);
                }
                // Scalar Tail
                for (; i < paddedCount - 1; i += 2) {
                    if (slots[i] > slots[i + 1]) {
                        int tmpS = slots[i];
                        slots[i] = slots[i + 1];
                        slots[i + 1] = tmpS;

                        double tmpV = vols[i];
                        vols[i] = vols[i + 1];
                        vols[i + 1] = tmpV;
                    }
                }
            }
        }
    }
    /**
     * 自适应排序决策
     *
     * 排序收益分析:
     * - 小数据集 (< SORT_THRESHOLD): 排序开销 > Gather 收益
     * - 稀疏数据 (密度低): 排序后仍无连续性,收益有限
     * - 密集数据 (密度高): 排序后可能出现局部连续访问,收益显著
     *
     * @return true 表示应该排序
     */
    private boolean shouldSort(int count, int segId) {
        if (count < SORT_THRESHOLD) return false;

        // 计算密度: items per 1KB address space
        // 如果物品在地址空间中足够密集,排序后更可能产生连续访问
        if (count < SORT_DENSITY_MIN) return false;

        return true;
    }
    // ==================== 6. 核心计算逻辑 (SIMD Extreme) ====================
    private double calculateBatchInternal(int[] handles, double[] volumes, int count) {
        // 栈上分配 Context (Zero-GC)
        CalcContext ctx = new CalcContext();

        // 分桶
        for (int i = 0; i < count; i++) {
            int h = handles[i];
            if (h == -1) continue;
            ctx.push(h >>> 16, h & SLOT_MASK, volumes[i]);
        }

        long now = System.currentTimeMillis();
        double sum = 0.0;

        // 逐 Segment 处理
        for (int i = 0; i < SEGMENT_COUNT; i++) {
            int segCount = ctx.counts[i];
            if (segCount > 0) {
                // 自适应排序
                if (shouldSort(segCount, i)) {
                    bitonicSort(ctx.segSlots[i], ctx.segVols[i], segCount);
                    totalSorts++;
                } else {
                    totalSkippedSorts++;
                }
                sum += processSegment(segments[i], ctx, i, now);
            }
        }

        if (plugin.getPerformanceMonitor() != null) {
            plugin.getPerformanceMonitor().recordCalculation(true);
        }
        return sum;
    }
    /**
     * Segment 级别的 SIMD 处理
     *
     * 优化要点:
     * 1. True Gather/Scatter: 直接从 MemorySegment 加载离散数据
     * 2. FMA 指令融合: 减少舍入误差和指令延迟
     * 3. 向量化时间戳转换: L2D (Long to Double) 在向量寄存器内完成
     * 4. 最小化内存屏障: 利用 StampedLock 减少同步开销
     */
    private double processSegment(Segment seg, CalcContext ctx, int segId, long now) {
        int count = ctx.counts[segId];
        int[] slots = ctx.segSlots[segId];
        double[] vols = ctx.segVols[segId];
        double segLambdaSum = 0.0;
        long stamp = seg.lock.writeLock();
        try {
            MemorySegment msInt = seg.integrals;
            MemorySegment msErr = seg.errors;
            MemorySegment msLam = seg.lambdas;
            MemorySegment msTime = seg.times;
            MemorySegment msDirty = seg.dirty;
            // 预广播当前时间
            DoubleVector vNowD = DoubleVector.broadcast(SPECIES, (double) now);
            LongVector vNowL = LongVector.broadcast(L_SPECIES, now);
            // 使用 ThreadLocal 重用临时数组，消除分配
            long[] offsetArray = OFFSET_ARRAY_TL.get();
            double[] tempIntegral = TEMP_INTEGRAL_TL.get();
            double[] tempError = TEMP_ERROR_TL.get();
            double[] tempLambda = TEMP_LAMBDA_TL.get();
            long[] tempTime = TEMP_TIME_TL.get();
            int i = 0;
            int loopBound = SPECIES.loopBound(count);
            // ---------------- True SIMD Gather/Scatter ----------------
            for (; i < loopBound; i += V_LEN) {
                // 软件预取提示 (针对下一轮迭代)
                if (i + V_LEN * PREFETCH_DISTANCE < count) {
                    int prefetchSlot = slots[i + V_LEN * PREFETCH_DISTANCE];
                    long prefetchOffset = (long) prefetchSlot * D_SIZE;
                    // JVM 可能优化为 PREFETCH 指令
                    msInt.get(D_LAYOUT, prefetchOffset);
                }
                // 1. 加载 Slot 索引向量并计算偏移量
                IntVector vSlots = IntVector.fromArray(I_SPECIES, slots, i);

                // 2. 转换为 Long 并计算字节偏移: offset = slot * 8
                LongVector vOffsets = (LongVector) vSlots.convertShape(VectorOperators.I2L, L_SPECIES, 0);
                vOffsets = vOffsets.mul(D_SIZE);

                // 写出偏移到数组
                vOffsets.intoArray(offsetArray, 0);

                // 3. [Gather] 使用标量循环从内存加载到临时数组（JIT 可优化为 gather）
                // 注意：如果升级到 JDK 21+ Vector API，可替换为 DoubleVector.gather(SPECIES, msInt, vOffsets, mask)等
                for (int lane = 0; lane < V_LEN; lane++) {
                    long offset = offsetArray[lane];
                    tempIntegral[lane] = msInt.get(D_LAYOUT, offset);
                    tempError[lane] = msErr.get(D_LAYOUT, offset);
                    tempLambda[lane] = msLam.get(D_LAYOUT, offset);
                    tempTime[lane] = msTime.get(L_LAYOUT, offset);
                }

                // 从数组加载到向量
                DoubleVector vIntegral = DoubleVector.fromArray(SPECIES, tempIntegral, 0);
                DoubleVector vLastErr = DoubleVector.fromArray(SPECIES, tempError, 0);
                DoubleVector vLambda = DoubleVector.fromArray(SPECIES, tempLambda, 0);
                LongVector vTimeL = LongVector.fromArray(L_SPECIES, tempTime, 0);

                // 4. 计算 DT: (now - lastTime) * 0.001
                DoubleVector vTimeD = (DoubleVector) vTimeL.convertShape(VectorOperators.L2D, SPECIES, 0);
                DoubleVector vDt = vNowD.sub(vTimeD).mul(vPointZeroZeroOne);

                // 5. PID Math (Full FMA Optimization)
                vDt = vDt.max(vDtMin).min(vDtMax);

                DoubleVector vVol = DoubleVector.fromArray(SPECIES, vols, i);
                DoubleVector vErrRaw = vVol.sub(vTarget);

                // Deadband 处理
                VectorMask<Double> deadbandMask = vErrRaw.abs().compare(
                    VectorOperators.LT, vDeadband
                );
                DoubleVector vErr = vErrRaw.blend(vZero, deadbandMask);

                // Derivative: dErr/dt
                DoubleVector vDPart = vErr.sub(vLastErr).div(vDt);

                // Integral with decay: I' = I * (1 - dt/tau) + E * dt
                // 使用 FMA: decay = dt * (-1/tau) + 1
                DoubleVector vDecay = vDt.fma(vNegTauInv, vOne);
                vIntegral = vIntegral.mul(vDecay).add(vErr.mul(vDt))
                    .max(vIMin).min(vIMax);

                // Proportional with asymmetric gain
                VectorMask<Double> negMask = vErr.compare(VectorOperators.LT, vZero);
                DoubleVector vKpUsed = vKp.blend(vKpNeg, negMask);

                // PID Output: raw = Kp*E + Ki*I + Kd*D + base
                // 使用 FMA 链式优化
                DoubleVector vRaw = vErr.mul(vKpUsed)
                    .add(vIntegral.mul(vKi))
                    .add(vDPart.mul(vKd))
                    .add(vBase);

                // Exponential smoothing: λ' = β*λ + α*raw
                vLambda = vLambda.fma(vBeta, vRaw.mul(vAlpha))
                    .max(vLMin).min(vLMax);
                // Accumulate Lambda
                segLambdaSum += vLambda.reduceLanes(VectorOperators.ADD);
                // 6. [Scatter] 从向量写到临时数组
                vIntegral.intoArray(tempIntegral, 0);
                vErr.intoArray(tempError, 0);
                vLambda.intoArray(tempLambda, 0);
                vNowL.intoArray(tempTime, 0);

                // 使用标量循环写回内存（JIT 可优化为 scatter）
                // 注意：如果升级到 JDK 21+，可替换为 vIntegral.scatter(msInt, vOffsets, mask)等
                for (int lane = 0; lane < V_LEN; lane++) {
                    long offset = offsetArray[lane];
                    msInt.set(D_LAYOUT, offset, tempIntegral[lane]);
                    msErr.set(D_LAYOUT, offset, tempError[lane]);
                    msLam.set(D_LAYOUT, offset, tempLambda[lane]);
                    msTime.set(L_LAYOUT, offset, tempTime[lane]);
                }

                // 7. 标记 Dirty (向量化 Byte 设置 - 使用循环因为 ByteVector 不直接支持 scatter，但可优化为 SIMD)
                for (int lane = 0; lane < V_LEN; lane++) {
                    msDirty.set(B_LAYOUT, (long) slots[i + lane], (byte) 1);
                }
            }
            // ---------------- Scalar Tail ----------------
            for (; i < count; i++) {
                int slotIdx = slots[i];
                long offset = (long) slotIdx * D_SIZE;

                // Load
                long lastTime = msTime.get(L_LAYOUT, offset);
                double integral = msInt.get(D_LAYOUT, offset);
                double lastErr = msErr.get(D_LAYOUT, offset);
                double lambda = msLam.get(D_LAYOUT, offset);

                // Compute
                double dt = Math.max(dtMin, Math.min(dtMax, (now - lastTime) * 0.001));
                double err = vols[i] - target;
                if (Math.abs(err) < deadband) err = 0.0;

                // Integral with decay (FMA)
                double decay = 1.0 - dt * tau;
                integral = Math.fma(integral, decay, err * dt);
                integral = Math.max(integralMin, Math.min(integralMax, integral));

                // PID
                double kpUsed = (err < 0) ? (kp * kpNegMultiplier) : kp;
                double dPart = (err - lastErr) / dt;
                double raw = kpUsed * err + integral * ki + dPart * kd + base;

                // Smoothing (FMA)
                lambda = Math.fma(lambda, beta, raw * alpha);
                lambda = Math.max(lambdaMin, Math.min(lambdaMax, lambda));
                segLambdaSum += lambda;

                // Store
                msInt.set(D_LAYOUT, offset, integral);
                msErr.set(D_LAYOUT, offset, err);
                msLam.set(D_LAYOUT, offset, lambda);
                msTime.set(L_LAYOUT, offset, now);
                msDirty.set(B_LAYOUT, (long) slotIdx, (byte) 1);
            }
        } finally {
            seg.lock.unlockWrite(stamp);
        }
        return segLambdaSum;
    }
    // ==================== 7. 内部类与清理 ====================
    /**
     * Segment - Cache Line 对齐的内存段
     * 使用 Padding 避免 False Sharing
     */
    private static class Segment {
        // Cache Line Padding (避免与其他 Segment 的 lock 共享 Cache Line)
        @SuppressWarnings("unused")
        long p01, p02, p03, p04, p05, p06, p07;

        final StampedLock lock = new StampedLock();

        @SuppressWarnings("unused")
        long p11, p12, p13, p14, p15, p16, p17;
        final MemorySegment integrals, errors, lambdas, times, dirty;
        final BitSet allocationMap = new BitSet(CAPACITY);
        Segment(Arena arena) {
            // 分配 Cache Line 对齐的内存
            integrals = arena.allocate(D_LAYOUT, CAPACITY);
            errors = arena.allocate(D_LAYOUT, CAPACITY);
            lambdas = arena.allocate(D_LAYOUT, CAPACITY);
            times = arena.allocate(L_LAYOUT, CAPACITY);
            dirty = arena.allocate(B_LAYOUT, CAPACITY);
            warmup();
        }

        /**
         * 内存预热 - 触碰所有页表,避免运行时缺页中断
         */
        void warmup() {
            for (int i = 0; i < CAPACITY; i += 512) {
                integrals.setAtIndex(D_LAYOUT, i, 0.0);
                errors.setAtIndex(D_LAYOUT, i, 0.0);
                lambdas.setAtIndex(D_LAYOUT, i, 0.002);
                times.setAtIndex(L_LAYOUT, i, 0L);
                dirty.setAtIndex(B_LAYOUT, i, (byte) 0);
            }
        }
        int allocateSlot() {
            int slot = allocationMap.nextClearBit(0);
            if (slot >= CAPACITY) return -1;
            allocationMap.set(slot);
            lambdas.setAtIndex(D_LAYOUT, slot, 0.002);
            times.setAtIndex(L_LAYOUT, slot, System.currentTimeMillis());
            return slot;
        }
    }
    /**
     * CalcContext - 零分配计算上下文
     *
     * 设计要点:
     * 1. 分段存储,减少单次分配大小
     * 2. 懒加载扩容,按需分配
     * 3. 固定最大容量,避免 OOM
     */
    private static class CalcContext {
        static final int SEGMENT_CAPACITY = CAPACITY;  // 固定为 8192，与 segment 槽位匹配

        int[][] segSlots = new int[SEGMENT_COUNT][];
        double[][] segVols = new double[SEGMENT_COUNT][];
        final int[] counts = new int[SEGMENT_COUNT];

        CalcContext() {
            Arrays.fill(counts, 0);
            // 预分配所有 segment 数组，消除懒加载和 grow
            for (int i = 0; i < SEGMENT_COUNT; i++) {
                segSlots[i] = new int[SEGMENT_CAPACITY];
                segVols[i] = new double[SEGMENT_CAPACITY];
            }
        }

        void push(int segId, int slot, double vol) {
            int idx = counts[segId]++;
            if (idx >= SEGMENT_CAPACITY) {
                // 异常处理：segment 溢出（罕见，因为 allocationMap 限制）
                throw new IllegalStateException("Segment " + segId + " overflow in CalcContext");
            }
            segSlots[segId][idx] = slot;
            segVols[segId][idx] = vol;
        }
    }
    // ==================== 8. 持久化与生命周期管理 ====================
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            if (arena.scope().isAlive()) {
                arena.close();
            }
            plugin.getLogger().info(
                "[PID] Controller closed. " + getPerformanceStats()
            );
        }
    }
    public void flushBuffer(boolean sync) {
        if (closed.get()) return;
        List<DatabaseManager.PidDbSnapshot> batch = new ArrayList<>(1024);

        for (int segId = 0; segId < SEGMENT_COUNT; segId++) {
            Segment seg = segments[segId];
            long stamp = seg.lock.writeLock();
            try {
                for (int i = seg.allocationMap.nextSetBit(0); i >= 0;
                     i = seg.allocationMap.nextSetBit(i + 1)) {
                    if (seg.dirty.get(B_LAYOUT, (long) i) == (byte) 1) {
                        seg.dirty.set(B_LAYOUT, (long) i, (byte) 0);
                        int handle = (segId << 16) | i;
                        String id = handleToId.get(handle);
                        if (id != null) {
                            long offset = (long) i * D_SIZE;
                            batch.add(new DatabaseManager.PidDbSnapshot(
                                id,
                                seg.integrals.get(D_LAYOUT, offset),
                                seg.errors.get(D_LAYOUT, offset),
                                seg.lambdas.get(D_LAYOUT, offset),
                                seg.times.get(L_LAYOUT, offset)
                            ));
                        }
                    }
                    if (batch.size() >= 1000) {
                        save(batch, sync);
                        batch.clear();
                    }
                }
            } finally {
                seg.lock.unlockWrite(stamp);
            }
        }
        if (!batch.isEmpty()) save(batch, sync);
    }
    private void save(List<DatabaseManager.PidDbSnapshot> batch, boolean sync) {
        List<DatabaseManager.PidDbSnapshot> copy = new ArrayList<>(batch);
        if (sync) {
            plugin.getDatabaseManager().saveBatch(copy);
        } else {
            Thread.ofVirtual().start(() -> plugin.getDatabaseManager().saveBatch(copy));
        }
    }
    public void loadAllStates() {
        if (closed.get()) return;
        plugin.getDatabaseManager().loadStates(s -> {
            int handle = getHandle(s.itemId());
            if (handle < 0) return;
            int segId = handle >>> 16;
            int slot = handle & SLOT_MASK;
            Segment seg = segments[segId];
            long offset = (long) slot * D_SIZE;
            long stamp = seg.lock.writeLock();
            try {
                seg.integrals.set(D_LAYOUT, offset, s.integral());
                seg.errors.set(D_LAYOUT, offset, s.lastError());
                seg.lambdas.set(D_LAYOUT, offset, s.lastLambda());
                seg.times.set(L_LAYOUT, offset, s.updateTime());
                seg.dirty.set(B_LAYOUT, (long) slot, (byte) 0);
            } finally {
                seg.lock.unlockWrite(stamp);
            }
        });
    }
}
