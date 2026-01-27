package top.ellan.ecobridge;

import jdk.incubator.vector.*;
import java.lang.foreign.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.StampedLock;

/**
 * 极致性能 PID 控制器 (Java 25 Vector API 合规终极修复版)
 * 
 * ✅ 核心修复：
 *   - convertShape 返回值显式强转为 DoubleVector（解决 Type mismatch）
 *   - 修正 fma 参数顺序：vErr.fma(vKpUsed, ...) → vKpUsed.fma(vErr, ...) 语义更清晰
 *   - 所有向量操作显式使用 DoubleVector/LongVector（杜绝 Vector<Double> 误用）
 *   - 保留物种长度安全校验（启动时验证）
 *   - 零分配 ThreadLocal 重用
 *   - 严格遵循 Panama FFM + Vector API 规范
 */
public class PidController implements AutoCloseable {
    // ==================== 1. 向量化与常量配置 (精简合规) ====================
    private static final VectorSpecies<Double> SPECIES = DoubleVector.SPECIES_PREFERRED;
    private static final int V_LEN = SPECIES.length();
    private static final VectorSpecies<Long> L_SPECIES = LongVector.SPECIES_PREFERRED;
    
    // 启动时校验物种长度一致性（关键安全措施）
    static {
        if (L_SPECIES.length() != V_LEN) {
            throw new IllegalStateException(
                "Vector species length mismatch: Double=" + V_LEN + ", Long=" + L_SPECIES.length() +
                ". Platform may require explicit species selection.");
        }
    }
    
    private static final int SEGMENT_BITS = 4;
    private static final int SEGMENT_COUNT = 1 << SEGMENT_BITS;
    private static final int SEGMENT_MASK = SEGMENT_COUNT - 1;
    private static final int CAPACITY = 8192;
    private static final int SLOT_MASK = 0xFFFF;
    private static final int SORT_THRESHOLD = 128;
    private static final int PREFETCH_DISTANCE = 8;
    
    private static final ValueLayout.OfDouble D_LAYOUT = ValueLayout.JAVA_DOUBLE;
    private static final ValueLayout.OfLong L_LAYOUT = ValueLayout.JAVA_LONG;
    private static final ValueLayout.OfByte B_LAYOUT = ValueLayout.JAVA_BYTE;
    private static final long D_SIZE = 8L;

    // ThreadLocal 重用（零分配热路径）
    private static final ThreadLocal<double[]> TEMP_INTEGRAL_TL = ThreadLocal.withInitial(() -> new double[V_LEN]);
    private static final ThreadLocal<double[]> TEMP_ERROR_TL = ThreadLocal.withInitial(() -> new double[V_LEN]);
    private static final ThreadLocal<double[]> TEMP_LAMBDA_TL = ThreadLocal.withInitial(() -> new double[V_LEN]);
    private static final ThreadLocal<long[]> TEMP_TIME_TL = ThreadLocal.withInitial(() -> new long[V_LEN]);
    private static final ThreadLocal<long[]> OFFSET_ARRAY_TL = ThreadLocal.withInitial(() -> new long[V_LEN]);

    // ==================== 2. PID 参数与向量化广播 ====================
    private double target, deadband, kp, kpNegMultiplier, ki, kd, base, alpha, beta, tau;
    private double integralMax, integralMin, lambdaMax, lambdaMin, dtMax, dtMin;
    private DoubleVector vTarget, vDeadband, vKp, vKpNeg, vKi, vKd, vBase, vAlpha, vBeta;
    private DoubleVector vZero, vOne, vIMax, vIMin, vLMax, vLMin, vDtMax, vDtMin, vNegTauInv, vPointZeroZeroOne;
    
    // ==================== 3. 核心成员 ====================
    private final EcoBridge plugin;
    private final Arena arena;
    private final Segment[] segments;
    private final ConcurrentHashMap<String, Integer> registry = new ConcurrentHashMap<>(4096);
    private final ConcurrentHashMap<Integer, String> handleToId = new ConcurrentHashMap<>(4096);
    private final AtomicBoolean closed = new AtomicBoolean(false);
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
            "[PID] Java 25 Optimized Controller Initialized. Vector Lane: %d, Species: %s",
            V_LEN, SPECIES.toString()
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
        
        // 向量化参数广播（显式使用 DoubleVector）
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
        this.vNegTauInv = DoubleVector.broadcast(SPECIES, -1.0 / tau);
        this.vPointZeroZeroOne = DoubleVector.broadcast(SPECIES, 0.001);
    }

    public void reloadConfig() {
        loadConfig();
        plugin.getLogger().info("[PID] Parameters reloaded (Java 25 Optimized).");
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

    public String getPerformanceStats() {
        long total = totalSorts + totalSkippedSorts;
        double sortRate = total > 0 ? (totalSorts * 100.0 / total) : 0.0;
        return String.format(
            "Sorts: %d, Skipped: %d, Rate: %.2f%% | Vector Lane: %d | Species: %s",
            totalSorts, totalSkippedSorts, sortRate, V_LEN, SPECIES.toString()
        );
    }

    // ==================== 5. 安全排序实现 (标量插入排序) ====================
    private void bitonicSort(int[] slots, double[] vols, int count) {
        if (count <= 1) return;
        for (int i = 1; i < count; i++) {
            int keySlot = slots[i];
            double keyVol = vols[i];
            int j = i - 1;
            while (j >= 0 && slots[j] > keySlot) {
                slots[j + 1] = slots[j];
                vols[j + 1] = vols[j];
                j--;
            }
            slots[j + 1] = keySlot;
            vols[j + 1] = keyVol;
        }
    }

    private boolean shouldSort(int count, int segId) {
        return count >= SORT_THRESHOLD;
    }

    // ==================== 6. 核心计算逻辑 (合规SIMD终极优化) ====================
    private double calculateBatchInternal(int[] handles, double[] volumes, int count) {
        CalcContext ctx = new CalcContext();
        for (int i = 0; i < count; i++) {
            int h = handles[i];
            if (h == -1) continue;
            ctx.push(h >>> 16, h & SLOT_MASK, volumes[i]);
        }
        
        long now = System.currentTimeMillis();
        double sum = 0.0;
        
        for (int i = 0; i < SEGMENT_COUNT; i++) {
            int segCount = ctx.counts[i];
            if (segCount > 0) {
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
     * Segment 级别 SIMD 处理 (合规终极方案)
     * 
     * 关键修复:
     * 1. convertShape 返回值显式强转为 DoubleVector（解决编译错误）
     * 2. fma 参数顺序修正：kpUsed * err + ... 语义更清晰
     * 3. 所有向量操作显式使用具体子类类型
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
            
            // 预广播当前时间（显式使用具体向量类型）
            DoubleVector vNowD = DoubleVector.broadcast(SPECIES, (double) now);
            LongVector vNowL = LongVector.broadcast(L_SPECIES, now);
            
            // 重用 ThreadLocal 临时数组（零分配）
            double[] tempIntegral = TEMP_INTEGRAL_TL.get();
            double[] tempError = TEMP_ERROR_TL.get();
            double[] tempLambda = TEMP_LAMBDA_TL.get();
            long[] tempTime = TEMP_TIME_TL.get();
            long[] offsetArray = OFFSET_ARRAY_TL.get();
            
            int i = 0;
            int loopBound = SPECIES.loopBound(count);
            
            // ---------------- 合规SIMD循环 (标量加载/存储 + 向量计算) ----------------
            for (; i < loopBound; i += V_LEN) {
                // 软件预取（针对下一轮迭代）
                if (i + V_LEN * PREFETCH_DISTANCE < count) {
                    int prefetchSlot = slots[i + V_LEN * PREFETCH_DISTANCE];
                    long prefetchOffset = (long) prefetchSlot * D_SIZE;
                    msInt.get(D_LAYOUT, prefetchOffset); // 触发JIT预取
                }
                
                // 1. 计算字节偏移量（标量循环，V_LEN极小）
                for (int lane = 0; lane < V_LEN; lane++) {
                    offsetArray[lane] = (long) slots[i + lane] * D_SIZE;
                }
                
                // 2. 【合规加载】标量循环 → 临时数组 → fromArray（零分配 + 硬件友好）
                for (int lane = 0; lane < V_LEN; lane++) {
                    tempIntegral[lane] = msInt.get(D_LAYOUT, offsetArray[lane]);
                    tempError[lane] = msErr.get(D_LAYOUT, offsetArray[lane]);
                    tempLambda[lane] = msLam.get(D_LAYOUT, offsetArray[lane]);
                    tempTime[lane] = msTime.get(L_LAYOUT, offsetArray[lane]);
                }
                // 显式声明具体向量类型（杜绝 Vector<Double> 误用）
                DoubleVector vIntegral = DoubleVector.fromArray(SPECIES, tempIntegral, 0);
                DoubleVector vLastErr = DoubleVector.fromArray(SPECIES, tempError, 0);
                DoubleVector vLambda = DoubleVector.fromArray(SPECIES, tempLambda, 0);
                LongVector vTimeL = LongVector.fromArray(L_SPECIES, tempTime, 0);
                
                // 3. 向量化时间戳转换 (L2D) - 【关键修复：显式强转】
                DoubleVector vTimeD = (DoubleVector) vTimeL.convertShape(VectorOperators.L2D, SPECIES, 0);
                DoubleVector vDt = vNowD.sub(vTimeD).mul(vPointZeroZeroOne);
                vDt = vDt.max(vDtMin).min(vDtMax);
                
                // 4. PID Math (Full FMA Optimization - 全向量化)
                DoubleVector vVol = DoubleVector.fromArray(SPECIES, vols, i);
                DoubleVector vErrRaw = vVol.sub(vTarget);
                VectorMask<Double> deadbandMask = vErrRaw.abs().compare(VectorOperators.LT, vDeadband);
                DoubleVector vErr = vErrRaw.blend(vZero, deadbandMask);
                DoubleVector vDPart = vErr.sub(vLastErr).div(vDt);
                DoubleVector vDecay = vDt.fma(vNegTauInv, vOne); // decay = 1 + dt*(-1/tau)
                vIntegral = vIntegral.mul(vDecay).add(vErr.mul(vDt)).max(vIMin).min(vIMax);
                VectorMask<Double> negMask = vErr.compare(VectorOperators.LT, vZero);
                DoubleVector vKpUsed = vKp.blend(vKpNeg, negMask);
                // 修正 fma 顺序：kpUsed * err + (integral * ki + (dPart * kd + base))
                DoubleVector vRaw = vKpUsed.fma(vErr, 
                    vKi.fma(vIntegral, 
                        vKd.fma(vDPart, vBase)));
                vLambda = vLambda.fma(vBeta, vRaw.mul(vAlpha)).max(vLMin).min(vLMax);
                segLambdaSum += vLambda.reduceLanes(VectorOperators.ADD);
                
                // 5. 【合规存储】intoArray → 标量循环写回（零分配 + 硬件友好）
                vIntegral.intoArray(tempIntegral, 0);
                vErr.intoArray(tempError, 0);
                vLambda.intoArray(tempLambda, 0);
                vNowL.intoArray(tempTime, 0);
                for (int lane = 0; lane < V_LEN; lane++) {
                    msInt.set(D_LAYOUT, offsetArray[lane], tempIntegral[lane]);
                    msErr.set(D_LAYOUT, offsetArray[lane], tempError[lane]);
                    msLam.set(D_LAYOUT, offsetArray[lane], tempLambda[lane]);
                    msTime.set(L_LAYOUT, offsetArray[lane], tempTime[lane]);
                    msDirty.set(B_LAYOUT, (long) slots[i + lane], (byte) 1); // Dirty标记（标量）
                }
            }
            
            // ---------------- Scalar Tail (安全兜底) ----------------
            for (; i < count; i++) {
                int slotIdx = slots[i];
                long offset = (long) slotIdx * D_SIZE;
                long lastTime = msTime.get(L_LAYOUT, offset);
                double integral = msInt.get(D_LAYOUT, offset);
                double lastErr = msErr.get(D_LAYOUT, offset);
                double lambda = msLam.get(D_LAYOUT, offset);
                double dt = Math.max(dtMin, Math.min(dtMax, (now - lastTime) * 0.001));
                double err = vols[i] - target;
                if (Math.abs(err) < deadband) err = 0.0;
                double decay = 1.0 - dt / tau;
                integral = Math.fma(integral, decay, err * dt);
                integral = Math.max(integralMin, Math.min(integralMax, integral));
                double kpUsed = (err < 0) ? (kp * kpNegMultiplier) : kp;
                double dPart = (err - lastErr) / dt;
                double raw = kpUsed * err + integral * ki + dPart * kd + base;
                lambda = Math.fma(lambda, beta, raw * alpha);
                lambda = Math.max(lambdaMin, Math.min(lambdaMax, lambda));
                segLambdaSum += lambda;
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
    private static class Segment {
        // Cache Line Padding (False Sharing 防护)
        @SuppressWarnings("unused") long p01, p02, p03, p04, p05, p06, p07;
        final StampedLock lock = new StampedLock();
        @SuppressWarnings("unused") long p11, p12, p13, p14, p15, p16, p17;
        
        final MemorySegment integrals, errors, lambdas, times, dirty;
        final BitSet allocationMap = new BitSet(CAPACITY);
        
        Segment(Arena arena) {
            integrals = arena.allocate(D_LAYOUT, CAPACITY);
            errors = arena.allocate(D_LAYOUT, CAPACITY);
            lambdas = arena.allocate(D_LAYOUT, CAPACITY);
            times = arena.allocate(L_LAYOUT, CAPACITY);
            dirty = arena.allocate(B_LAYOUT, CAPACITY);
            warmup();
        }
        
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
    
    private static class CalcContext {
        static final int SEGMENT_CAPACITY = CAPACITY;
        int[][] segSlots = new int[SEGMENT_COUNT][];
        double[][] segVols = new double[SEGMENT_COUNT][];
        final int[] counts = new int[SEGMENT_COUNT];
        
        CalcContext() {
            Arrays.fill(counts, 0);
            for (int i = 0; i < SEGMENT_COUNT; i++) {
                segSlots[i] = new int[SEGMENT_CAPACITY];
                segVols[i] = new double[SEGMENT_CAPACITY];
            }
        }
        
        void push(int segId, int slot, double vol) {
            int idx = counts[segId]++;
            if (idx >= SEGMENT_CAPACITY) {
                throw new IllegalStateException("Segment " + segId + " overflow in CalcContext");
            }
            segSlots[segId][idx] = slot;
            segVols[segId][idx] = vol;
        }
    }
    
    // ==================== 8. 生命周期管理 ====================
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            if (arena.scope().isAlive()) {
                arena.close();
            }
            plugin.getLogger().info(
                "[PID] Controller closed (Java 25 Optimized). " + getPerformanceStats()
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