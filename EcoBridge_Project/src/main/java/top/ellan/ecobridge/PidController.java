package top.ellan.ecobridge;

import jdk.incubator.vector.*;
import java.lang.foreign.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.StampedLock;

/**
 * PidController (Ultimate Optimized Edition with Configurable PID Parameters + O(1) Reverse Lookup)
 *
 * 核心优化:
 * 1. Handle 机制 + 双向映射 (registry + handleToId) → flushBuffer 变为 O(1)
 * 2. 完全可配置的 PID 参数（从 config.yml 读取）
 * 3. Zero-GC: ThreadLocal 上下文缓冲 + 自动扩容数组
 * 4. Data Swizzling + SIMD + FMA + ILP
 * 5. False Sharing 防护 (Cache Line Padding)
 */
public class PidController implements AutoCloseable {

    // ==================== 1. 向量化与常量配置 ====================
    private static final VectorSpecies<Double> SPECIES = DoubleVector.SPECIES_PREFERRED;
    private static final int V_LEN = SPECIES.length();

    // 分段配置 (16 Segments)
    private static final int SEGMENT_BITS = 4;
    private static final int SEGMENT_COUNT = 1 << SEGMENT_BITS;
    private static final int SEGMENT_MASK = SEGMENT_COUNT - 1;

    // 容量配置 (每段 8192 个槽位，总容量 131,072)
    private static final int CAPACITY = 8192;
    private static final int SLOT_MASK = 0xFFFF; // Handle 解析掩码

    // FFM 内存布局
    private static final ValueLayout.OfDouble D_LAYOUT = ValueLayout.JAVA_DOUBLE;
    private static final ValueLayout.OfLong L_LAYOUT = ValueLayout.JAVA_LONG;
    private static final ValueLayout.OfByte B_LAYOUT = ValueLayout.JAVA_BYTE;
    private static final long D_SIZE = 8L;

    // ==================== 2. 可配置 PID 参数（实例字段） ====================
    private double target;
    private double deadband;
    private double kp;
    private double kpNegativeMultiplier;
    private double ki;
    private double kd;
    private double base;
    private double alpha;
    private double beta;
    private double tau;
    private double integralMax;
    private double integralMin;
    private double lambdaMax;
    private double lambdaMin;
    private double dtMax;
    private double dtMin;

    // 向量化常量（运行时生成）
    private DoubleVector vTarget;
    private DoubleVector vDeadband;
    private DoubleVector vKp;
    private DoubleVector vKpNeg;
    private DoubleVector vKi;
    private DoubleVector vKd;
    private DoubleVector vBase;
    private DoubleVector vAlpha;
    private DoubleVector vBeta;
    private DoubleVector vZero;
    private DoubleVector vOne;
    private DoubleVector vIMax;
    private DoubleVector vIMin;
    private DoubleVector vLMax;
    private DoubleVector vLMin;
    private DoubleVector vDtMax;
    private DoubleVector vDtMin;
    private DoubleVector vNegTauInv;

    // ==================== 3. 核心成员 ====================
    private final EcoBridge plugin;
    private final Arena arena;
    private final Segment[] segments;

    // 注册表（正向：itemId → handle）
    private final ConcurrentHashMap<String, Integer> registry = new ConcurrentHashMap<>(4096);
    // 反向映射（handle → itemId）← 解决 flushBuffer 瓶颈
    private final ConcurrentHashMap<Integer, String> handleToId = new ConcurrentHashMap<>(4096);

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final ThreadLocal<CalcContext> tlContext = ThreadLocal.withInitial(CalcContext::new);

    public PidController(EcoBridge plugin) {
        this.plugin = plugin;
        this.arena = Arena.ofShared();
        this.segments = new Segment[SEGMENT_COUNT];
        for (int i = 0; i < SEGMENT_COUNT; i++) {
            this.segments[i] = new Segment(arena);
        }
        loadConfig();
        plugin.getLogger().info("[PID] Ultimate Optimized Controller Initialized. Vector Lane: " + V_LEN + ", Target: " + target);
    }

    private void loadConfig() {
        var cfg = plugin.getConfig();
        this.target = cfg.getDouble("pid.target", 1000.0);
        this.deadband = cfg.getDouble("pid.deadband", 20.0);
        this.kp = cfg.getDouble("pid.kp", 0.00001);
        this.kpNegativeMultiplier = cfg.getDouble("pid.kp-negative-multiplier", 0.6);
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

        // 生成向量常量
        this.vTarget = DoubleVector.broadcast(SPECIES, target);
        this.vDeadband = DoubleVector.broadcast(SPECIES, deadband);
        this.vKp = DoubleVector.broadcast(SPECIES, kp);
        this.vKpNeg = DoubleVector.broadcast(SPECIES, kp * kpNegativeMultiplier);
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
    }

    public void reloadConfig() {
        loadConfig();
        plugin.getLogger().info("[PID] PID parameters reloaded from config");
    }

    // ==================== 4. 注册 API (Cold Path) ====================
    public int getHandle(String itemId) {
        return registry.computeIfAbsent(itemId, id -> {
            int hash = Math.abs(id.hashCode());
            int segId = hash & SEGMENT_MASK;
            Segment seg = segments[segId];

            long stamp = seg.lock.writeLock();
            try {
                int slot = seg.allocateSlot();
                if (slot < 0) {
                    plugin.getLogger().warning("[PID] Segment " + segId + " overflow!");
                    return -1;
                }
                int handle = (segId << 16) | slot;
                handleToId.put(handle, id); // O(1) 反向映射
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
        double lambda = seg.lambdas.get(D_LAYOUT, offset);
        if (!seg.lock.validate(stamp)) {
            stamp = seg.lock.readLock();
            try {
                lambda = seg.lambdas.get(D_LAYOUT, offset);
            } finally {
                seg.lock.unlockRead(stamp);
            }
        }
        return lambda;
    }

    // ==================== 5. 批量计算 API (Hot Path) ====================
    public void calculateBatch(int[] handles, double[] volumes, int count) {
        if (closed.get() || count == 0) return;

        CalcContext ctx = tlContext.get();
        ctx.reset();

        for (int i = 0; i < count; i++) {
            int h = handles[i];
            if (h == -1) continue;
            int segId = h >>> 16;
            int slot = h & SLOT_MASK;
            ctx.push(segId, slot, volumes[i]);
        }

        long now = System.currentTimeMillis();
        for (int i = 0; i < SEGMENT_COUNT; i++) {
            if (ctx.counts[i] > 0) {
                processSegment(segments[i], ctx, i, now);
            }
        }

        if (plugin.getPerformanceMonitor() != null) {
            // 因为这是批量计算，我们记录一次高质量的 SIMD 计算
            plugin.getPerformanceMonitor().recordCalculation(true);
        }
    }

    // ==================== 6. 核心计算逻辑 (SIMD + FMA + ILP) ====================
    private void processSegment(Segment seg, CalcContext ctx, int segId, long now) {
        int count = ctx.counts[segId];
        int[] slots = ctx.segSlots[segId];
        double[] vols = ctx.segVols[segId];

        long stamp = seg.lock.writeLock();
        try {
            MemorySegment msInt = seg.integrals;
            MemorySegment msErr = seg.errors;
            MemorySegment msLam = seg.lambdas;
            MemorySegment msTime = seg.times;
            MemorySegment msDirty = seg.dirty;

            int i = 0;
            int loopBound = SPECIES.loopBound(count);
            double[] vBuf = ctx.vBufState;

            // SIMD 主循环
            for (; i < loopBound; i += V_LEN) {
                // Gather
                for (int lane = 0; lane < V_LEN; lane++) {
                    int slotIdx = slots[i + lane];
                    long offset = (long) slotIdx * D_SIZE;
                    vBuf[lane] = msInt.get(D_LAYOUT, offset);
                    vBuf[lane + V_LEN] = msErr.get(D_LAYOUT, offset);
                    vBuf[lane + 2 * V_LEN] = msLam.get(D_LAYOUT, offset);
                    long lastT = msTime.get(L_LAYOUT, offset);
                    vBuf[lane + 3 * V_LEN] = (now - lastT) * 0.001;
                }

                DoubleVector vIntegral = DoubleVector.fromArray(SPECIES, vBuf, 0);
                DoubleVector vLastErr = DoubleVector.fromArray(SPECIES, vBuf, V_LEN);
                DoubleVector vLambda = DoubleVector.fromArray(SPECIES, vBuf, 2 * V_LEN);
                DoubleVector vDt = DoubleVector.fromArray(SPECIES, vBuf, 3 * V_LEN);
                DoubleVector vVol = DoubleVector.fromArray(SPECIES, vols, i);

                // Clamp DT
                vDt = vDt.max(vDtMin).min(vDtMax);

                // Error + Deadband
                DoubleVector vErrRaw = vVol.sub(vTarget);
                VectorMask<Double> maskDead = vErrRaw.abs().compare(VectorOperators.LT, vDeadband);
                DoubleVector vErr = vErrRaw.blend(vZero, maskDead);

                // D Term (提前发射除法)
                DoubleVector vDPart = vErr.sub(vLastErr).div(vDt);

                // Integral Decay (FMA)
                DoubleVector vDecay = vDt.fma(vNegTauInv, vOne);
                vIntegral = vIntegral.mul(vDecay);
                vIntegral = vErr.fma(vDt, vIntegral);
                vIntegral = vIntegral.max(vIMin).min(vIMax);

                // P Term (Branchless)
                VectorMask<Double> maskNeg = vErr.compare(VectorOperators.LT, vZero);
                DoubleVector vKpUsed = vKp.blend(vKpNeg, maskNeg);
                DoubleVector vP = vErr.mul(vKpUsed);

                // D & I Term
                DoubleVector vD = vDPart.mul(vKd);
                DoubleVector vI = vIntegral.mul(vKi);

                // Raw Lambda
                DoubleVector vRaw = vP.add(vI).add(vD).add(vBase);

                // Lambda EMA (FMA)
                vLambda = vLambda.fma(vBeta, vRaw.mul(vAlpha));
                vLambda = vLambda.max(vLMin).min(vLMax);

                // Scatter
                vIntegral.intoArray(vBuf, 0);
                vErr.intoArray(vBuf, V_LEN);
                vLambda.intoArray(vBuf, 2 * V_LEN);

                for (int lane = 0; lane < V_LEN; lane++) {
                    int slotIdx = slots[i + lane];
                    long offset = (long) slotIdx * D_SIZE;
                    msInt.set(D_LAYOUT, offset, vBuf[lane]);
                    msErr.set(D_LAYOUT, offset, vBuf[lane + V_LEN]);
                    msLam.set(D_LAYOUT, offset, vBuf[lane + 2 * V_LEN]);
                    msTime.set(L_LAYOUT, offset, now);
                    msDirty.set(B_LAYOUT, (long) slotIdx, (byte) 1);
                }
            }

            // Scalar Tail
            for (; i < count; i++) {
                int slotIdx = slots[i];
                long offset = (long) slotIdx * D_SIZE;

                double dt = (now - msTime.get(L_LAYOUT, offset)) * 0.001;
                dt = Math.max(dtMin, Math.min(dtMax, dt));

                double integral = msInt.get(D_LAYOUT, offset);
                integral *= (1.0 - dt * tau);

                double vol = vols[i];
                double err = vol - target;
                if (Math.abs(err) < deadband) err = 0.0;

                double kpUsed = (err < 0) ? kp * kpNegativeMultiplier : kp;
                integral = Math.fma(err, dt, integral);
                integral = Math.max(integralMin, Math.min(integralMax, integral));

                double lastErr = msErr.get(D_LAYOUT, offset);
                double d = (err - lastErr) / dt * kd;

                double raw = kpUsed * err + integral * ki + d + base;
                double lambda = msLam.get(D_LAYOUT, offset);
                lambda = Math.fma(lambda, beta, raw * alpha);
                lambda = Math.max(lambdaMin, Math.min(lambdaMax, lambda));

                msInt.set(D_LAYOUT, offset, integral);
                msErr.set(D_LAYOUT, offset, err);
                msLam.set(D_LAYOUT, offset, lambda);
                msTime.set(L_LAYOUT, offset, now);
                msDirty.set(B_LAYOUT, (long) slotIdx, (byte) 1);
            }
        } finally {
            seg.lock.unlockWrite(stamp);
        }
    }

    // ==================== 7. 内部数据结构 ====================
    private static class Segment {
        @SuppressWarnings("unused")
        long p01, p02, p03, p04, p05, p06, p07;
        final StampedLock lock = new StampedLock();
        @SuppressWarnings("unused")
        long p11, p12, p13, p14, p15, p16, p17;

        final MemorySegment integrals;
        final MemorySegment errors;
        final MemorySegment lambdas;
        final MemorySegment times;
        final MemorySegment dirty;
        final BitSet allocationMap = new BitSet(CAPACITY);

        Segment(Arena arena) {
            integrals = arena.allocate(D_LAYOUT, CAPACITY);
            errors = arena.allocate(D_LAYOUT, CAPACITY);
            lambdas = arena.allocate(D_LAYOUT, CAPACITY);
            times = arena.allocate(L_LAYOUT, CAPACITY);
            dirty = arena.allocate(B_LAYOUT, CAPACITY);
            warmup();
        }

        int allocateSlot() {
            int slot = allocationMap.nextClearBit(0);
            if (slot >= CAPACITY) return -1;
            allocationMap.set(slot);
            lambdas.setAtIndex(D_LAYOUT, slot, 0.002); // 默认 lambda
            times.setAtIndex(L_LAYOUT, slot, System.currentTimeMillis());
            return slot;
        }

        void warmup() {
            for (int i = 0; i < CAPACITY; i += 512) {
                integrals.setAtIndex(D_LAYOUT, i, 0);
            }
        }
    }

    private static class CalcContext {
        static final int INITIAL_CAPACITY = 512;
        static final int MAX_CAPACITY = 1024 * 1024;

        int[][] segSlots = new int[SEGMENT_COUNT][INITIAL_CAPACITY];
        double[][] segVols = new double[SEGMENT_COUNT][INITIAL_CAPACITY];
        final int[] counts = new int[SEGMENT_COUNT];
        final double[] vBufState = new double[4 * V_LEN];

        void reset() {
            Arrays.fill(counts, 0);
        }

        void push(int segId, int slot, double vol) {
            int idx = counts[segId]++;
            if (idx >= segSlots[segId].length) {
                grow(segId);
            }
            segSlots[segId][idx] = slot;
            segVols[segId][idx] = vol;
        }

        private void grow(int segId) {
            int oldCap = segSlots[segId].length;
            if (oldCap >= MAX_CAPACITY) throw new OutOfMemoryError("PID Context buffer limit");
            int newCap = oldCap + (oldCap >> 1);
            if (newCap > MAX_CAPACITY) newCap = MAX_CAPACITY;

            int[] newSlots = new int[newCap];
            double[] newVols = new double[newCap];
            System.arraycopy(segSlots[segId], 0, newSlots, 0, oldCap);
            System.arraycopy(segVols[segId], 0, newVols, 0, oldCap);
            segSlots[segId] = newSlots;
            segVols[segId] = newVols;
        }
    }

    // ==================== 8. 生命周期与诊断 ====================
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            if (arena.scope().isAlive()) arena.close();
        }
    }

    public int getCacheSize() {
        return registry.size();
    }

    public int getDirtyQueueSize() {
        int totalDirty = 0;
        for (Segment seg : segments) {
            long stamp = seg.lock.readLock();
            try {
                for (int i = seg.allocationMap.nextSetBit(0); i >= 0; i = seg.allocationMap.nextSetBit(i + 1)) {
                    if (seg.dirty.get(B_LAYOUT, (long) i) == (byte) 1) {
                        totalDirty++;
                    }
                }
            } finally {
                seg.lock.unlockRead(stamp);
            }
        }
        return totalDirty;
    }

    public void flushBuffer(boolean sync) {
        if (closed.get()) return;
        List<DatabaseManager.PidDbSnapshot> batch = new ArrayList<>();

        for (int segId = 0; segId < SEGMENT_COUNT; segId++) {
            Segment seg = segments[segId];
            long stamp = seg.lock.writeLock();
            try {
                for (int i = seg.allocationMap.nextSetBit(0); i >= 0; i = seg.allocationMap.nextSetBit(i + 1)) {
                    if (seg.dirty.get(B_LAYOUT, (long) i) == (byte) 1) {
                        seg.dirty.set(B_LAYOUT, (long) i, (byte) 0);
                        int targetHandle = (segId << 16) | i;
                        String itemId = handleToId.get(targetHandle); // O(1) 反向查找
                        if (itemId != null) {
                            long offset = (long) i * D_SIZE;
                            batch.add(new DatabaseManager.PidDbSnapshot(
                                    itemId,
                                    seg.integrals.get(D_LAYOUT, offset),
                                    seg.errors.get(D_LAYOUT, offset),
                                    seg.lambdas.get(D_LAYOUT, offset),
                                    seg.times.get(L_LAYOUT, offset)
                            ));
                        }
                    }
                    if (batch.size() >= 1000) break;
                }
            } finally {
                seg.lock.unlockWrite(stamp);
            }
            if (batch.size() >= 1000) break;
        }

        if (batch.isEmpty()) return;

        if (sync) {
            plugin.getDatabaseManager().saveBatch(batch);
        } else {
            Thread.ofVirtual().start(() -> plugin.getDatabaseManager().saveBatch(batch));
        }
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

    public record PidStateDto(double integral, double lastError, double lastLambda, long updateTime) {}

    public void loadAllStates() {
        if (closed.get()) return;
        plugin.getDatabaseManager().loadStates(snapshot -> {
            int handle = getHandle(snapshot.itemId());
            if (handle < 0) return;
            int segId = handle >>> 16;
            int slot = handle & SLOT_MASK;
            Segment seg = segments[segId];
            long offset = (long) slot * D_SIZE;
            long stamp = seg.lock.writeLock();
            try {
                seg.integrals.set(D_LAYOUT, offset, snapshot.integral());
                seg.errors.set(D_LAYOUT, offset, snapshot.lastError());
                seg.lambdas.set(D_LAYOUT, offset, snapshot.lastLambda());
                seg.times.set(L_LAYOUT, offset, snapshot.updateTime());
                seg.dirty.set(B_LAYOUT, (long) slot, (byte) 0);
            } finally {
                seg.lock.unlockWrite(stamp);
            }
        });
    }
}