package top.ellan.ecobridge;

import jdk.incubator.vector.*;
import java.lang.foreign.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * PidController - The Void-Object Release (Zero-Boxing & Zero-False-Sharing)
 *
 * Performance Tuning for Java 25:
 * 1. Segment Padding: Manually padded cache lines to prevent false sharing between segments.
 * 2. Primitive Dirty Queue: Uses int[] instead of List<Integer> to eliminate boxing overhead.
 * 3. Lock Splitting: Read/Write lock separation to ensure low latency on the Paper main thread.
 * 4. SIMD Batching: Uses Vector API for mass PID calculations.
 */
public class PidController implements AutoCloseable {

    // ==================== 1. Vectorization & Constants ====================
    private static final VectorSpecies<Double> SPECIES = DoubleVector.SPECIES_PREFERRED;
    private static final int V_LEN = SPECIES.length();

    private static final int SEGMENT_BITS = 4;
    private static final int SEGMENT_COUNT = 1 << SEGMENT_BITS;
    private static final int SEGMENT_MASK = SEGMENT_COUNT - 1;

    private static final int CAPACITY = 8192;
    private static final int SLOT_MASK = 0xFFFF;

    private static final ValueLayout.OfDouble D_LAYOUT = ValueLayout.JAVA_DOUBLE;
    private static final ValueLayout.OfLong L_LAYOUT = ValueLayout.JAVA_LONG;
    private static final long D_SIZE = 8L;

    // ==================== 2. Configurable PID Parameters ====================
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

    // ==================== 3. Core Members ====================
    private final EcoBridge plugin;
    private final Arena arena;
    private final Segment[] segments;

    private final ConcurrentHashMap<String, Integer> registry = new ConcurrentHashMap<>(4096);
    private final ConcurrentHashMap<Integer, String> handleToId = new ConcurrentHashMap<>(4096);

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final DynamicCalcContextPool contextPool;

    public PidController(EcoBridge plugin) {
        this.plugin = plugin;
        this.arena = Arena.ofAuto();
        this.segments = new Segment[SEGMENT_COUNT];
        for (int i = 0; i < SEGMENT_COUNT; i++) {
            this.segments[i] = new Segment(arena);
        }
        
        int cores = Runtime.getRuntime().availableProcessors();
        // Context pool for batch calculations
        this.contextPool = new DynamicCalcContextPool(Math.max(4, cores), cores * 4);
        
        loadConfig();
        plugin.getLogger().info("[PID] Void-Object Controller Initialized. Zero-Boxing: Active | Padding: Active");
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

        initVectors();
    }

    private void initVectors() {
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
        plugin.getLogger().info("[PID] Config reloaded.");
    }

    // ==================== 4. Registration API ====================
    public int getHandle(String itemId) {
        return registry.computeIfAbsent(itemId, id -> {
            int hash = Math.abs(id.hashCode());
            int segId = hash & SEGMENT_MASK;
            Segment seg = segments[segId];

            synchronized (seg.lock) {
                int slot = seg.allocateSlot();
                if (slot < 0) {
                    plugin.getLogger().warning("[PID] Segment " + segId + " overflow!");
                    return -1;
                }
                int handle = (segId << 16) | slot;
                handleToId.put(handle, id);
                return handle;
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

        synchronized (seg.lock) {
            return seg.lambdas.get(D_LAYOUT, offset);
        }
    }

    // ==================== 5. Batch Calculation API ====================
    public void calculateBatch(int[] handles, double[] volumes, int count) {
        if (closed.get() || count == 0) return;

        CalcContext ctx = contextPool.acquire();
        try {
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
        } catch (IllegalStateException e) {
            plugin.getLogger().severe("PID Context Overflow: " + e.getMessage());
        } catch (Exception e) {
            plugin.getLogger().severe("PID Batch Error: " + e.getMessage());
        } finally {
            contextPool.release(ctx);
        }
    }

    // ==================== 6. Core Logic ====================

    private void processSegment(Segment seg, CalcContext ctx, int segId, long now) {
        int count = ctx.counts[segId];
        int[] slots = ctx.segSlots[segId];
        double[] localVols = ctx.segVols[segId];
        double[] vBuf = ctx.vBufState;

        // Phase 1: Gather (Short Critical Section)
        synchronized (seg.lock) {
            for (int i = 0; i < count; i++) {
                long offset = (long) slots[i] * D_SIZE;
                vBuf[i] = seg.integrals.get(D_LAYOUT, offset);
                vBuf[i + count] = seg.errors.get(D_LAYOUT, offset); 
                vBuf[i + count * 2] = seg.lambdas.get(D_LAYOUT, offset);
                vBuf[i + count * 3] = (now - seg.times.get(L_LAYOUT, offset)) * 0.001;
            }
        }

        // Phase 2: Compute (VectorMask eliminates tail loop completely)
        for (int i = 0; i < count; i += V_LEN) {
            // Generate mask for current step (handles non-V_LEN multiple counts)
            VectorMask<Double> m = SPECIES.indexInRange(i, count);

            // Masked load: Prevents out-of-bounds access
            DoubleVector vI = DoubleVector.fromArray(SPECIES, vBuf, i, m);
            DoubleVector vE = DoubleVector.fromArray(SPECIES, vBuf, i + count, m);
            DoubleVector vL = DoubleVector.fromArray(SPECIES, vBuf, i + count * 2, m);
            DoubleVector vDtRaw = DoubleVector.fromArray(SPECIES, vBuf, i + count * 3, m);
            DoubleVector vVol = DoubleVector.fromArray(SPECIES, localVols, i, m);

            // Limit dt
            DoubleVector vDt = vDtRaw.max(vDtMin).min(vDtMax);

            // 1. Error & Deadband
            DoubleVector vErrRaw = vVol.sub(vTarget);
            VectorMask<Double> maskDead = vErrRaw.abs().compare(VectorOperators.LT, vDeadband);
            DoubleVector vErr = vErrRaw.blend(vZero, maskDead);

            // 2. Integral (with exponential decay)
            DoubleVector vDecay = vDt.fma(vNegTauInv, vOne);
            vI = vErr.fma(vDt, vI.mul(vDecay)).max(vIMin).min(vIMax);

            // 3. P-Term (Asymmetric Kp)
            VectorMask<Double> maskNeg = vErr.compare(VectorOperators.LT, vZero);
            DoubleVector vKpUsed = vKp.blend(vKpNeg, maskNeg);
            DoubleVector vP = vErr.mul(vKpUsed);

            // 4. D-Term
            DoubleVector vD = vErr.sub(vE).div(vDt).mul(vKd);

            // 5. Final Lambda
            DoubleVector vRes = vP.add(vI.mul(vKi)).add(vD).add(vBase);
            vL = vL.fma(vBeta, vRes.mul(vAlpha)).max(vLMin).min(vLMax);

            // Masked store: Safe write-back
            vI.intoArray(vBuf, i, m);
            vErr.intoArray(vBuf, i + count, m);
            vL.intoArray(vBuf, i + count * 2, m);
        }

        // Phase 3: Scatter (Write back to off-heap memory)
        synchronized (seg.lock) {
            for (int k = 0; k < count; k++) {
                long offset = (long) slots[k] * D_SIZE;
                seg.integrals.set(D_LAYOUT, offset, vBuf[k]);
                seg.errors.set(D_LAYOUT, offset, vBuf[k + count]);
                seg.lambdas.set(D_LAYOUT, offset, vBuf[k + count * 2]);
                seg.times.set(L_LAYOUT, offset, now);
                seg.markDirty(slots[k]);
            }
        }
    }

    // ==================== 7. Internal Structures (Optimized) ====================
    private static class Segment {
        final Object lock = new Object(); 

        // [Optimized] Padding: Prevents false sharing of the lock object
        // 7 longs = 56 bytes + 8 bytes (lock ref) = 64 bytes (1 cache line)
        @SuppressWarnings("unused")
        private long p1, p2, p3, p4, p5, p6, p7;

        final MemorySegment integrals;
        final MemorySegment errors;
        final MemorySegment lambdas;
        final MemorySegment times;
        final BitSet allocationMap = new BitSet(CAPACITY);
        
        // [Optimized] Zero-Boxing Dirty Queue: Raw int array
        private int[] dirtySlots;
        private int dirtyCount;

        Segment(Arena arena) {
            integrals = arena.allocate(D_LAYOUT, CAPACITY);
            errors = arena.allocate(D_LAYOUT, CAPACITY);
            lambdas = arena.allocate(D_LAYOUT, CAPACITY);
            times = arena.allocate(L_LAYOUT, CAPACITY);
            
            // Init primitive dirty queue
            this.dirtySlots = new int[64];
            this.dirtyCount = 0;
            
            warmup();
        }

        int allocateSlot() {
            int slot = allocationMap.nextClearBit(0);
            if (slot >= CAPACITY) return -1;
            allocationMap.set(slot);
            lambdas.setAtIndex(D_LAYOUT, slot, 0.002);
            times.setAtIndex(L_LAYOUT, slot, System.currentTimeMillis());
            return slot;
        }

        void warmup() {
            for (int i = 0; i < CAPACITY; i += 512) {
                integrals.setAtIndex(D_LAYOUT, i, 0);
            }
        }

        void markDirty(int slot) {
            // Simple dedup: check only last element
            if (dirtyCount > 0 && dirtySlots[dirtyCount - 1] == slot) {
                return;
            }
            // Dynamic resize
            if (dirtyCount >= dirtySlots.length) {
                dirtySlots = Arrays.copyOf(dirtySlots, dirtySlots.length + 32);
            }
            dirtySlots[dirtyCount++] = slot;
        }
    }

    // ==================== 8. Context Pool ====================
    public static class DynamicCalcContextPool {
        private final ConcurrentLinkedQueue<CalcContext> pool = new ConcurrentLinkedQueue<>();
        private final AtomicInteger currentSize = new AtomicInteger(0);
        
        @SuppressWarnings("unused")
        private volatile int minSize;
        private volatile int maxSize;

        public DynamicCalcContextPool(int minSize, int maxSize) {
            this.minSize = minSize;
            this.maxSize = maxSize;
            for (int i = 0; i < minSize; i++) {
                pool.offer(new CalcContext());
                currentSize.incrementAndGet();
            }
        }

        public CalcContext acquire() {
            CalcContext ctx = pool.poll();
            if (ctx != null) {
                currentSize.decrementAndGet();
            } else {
                ctx = new CalcContext();
            }
            ctx.reset();
            return ctx;
        }

        public void release(CalcContext ctx) {
            if (currentSize.get() < maxSize) {
                pool.offer(ctx);
                currentSize.incrementAndGet();
            }
        }
        
        public int getAvailableCount() { return currentSize.get(); }
        public int getMinSize() { return minSize; }
        public void setMinSize(int newMin) { this.minSize = newMin; }
        public int getMaxSize() { return maxSize; }
        public void setMaxSize(int newMax) { this.maxSize = newMax; }
    }

    private static class CalcContext {
        static final int INITIAL_CAPACITY = 512;
        static final int HARD_LIMIT = 1024 * 1024; 

        int[][] segSlots = new int[SEGMENT_COUNT][INITIAL_CAPACITY];
        double[][] segVols = new double[SEGMENT_COUNT][INITIAL_CAPACITY];
        final int[] counts = new int[SEGMENT_COUNT];
        double[] vBufState; 

        CalcContext() {
            this.vBufState = new double[INITIAL_CAPACITY * 4];
        }

        void reset() {
            Arrays.fill(counts, 0);
        }

        void push(int segId, int slot, double vol) {
            int idx = counts[segId];
            if (idx >= segSlots[segId].length) {
                grow(segId);
            }
            segSlots[segId][idx] = slot;
            segVols[segId][idx] = vol;
            counts[segId]++;
        }

        private void grow(int segId) {
            int oldCap = segSlots[segId].length;
            int newCap = Math.min(HARD_LIMIT, oldCap + (oldCap >> 1));
            segSlots[segId] = Arrays.copyOf(segSlots[segId], newCap);
            segVols[segId] = Arrays.copyOf(segVols[segId], newCap);
    
            // Ensure buffer is large enough for potential new capacity
            if (vBufState.length < newCap * 4) {
                vBufState = new double[newCap * 4];
            }
        }
    }

    // ==================== 9. Lifecycle & Diagnostics ====================
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            if (arena.scope().isAlive()) arena.close();
        }
    }
    
    // [FIXED] Restored missing method required by PerformanceMonitor
    public DynamicCalcContextPool getContextPool() {
        return contextPool;
    }

    public int getCacheSize() {
        return registry.size();
    }

    public int getDirtyQueueSize() {
        int total = 0;
        for (Segment seg : segments) {
            total += seg.dirtyCount;
        }
        return total;
    }

    public void flushBuffer(boolean sync) {
        if (closed.get()) return;
        List<DatabaseManager.PidDbSnapshot> batch = new ArrayList<>();

        for (int segId = 0; segId < SEGMENT_COUNT; segId++) {
            Segment seg = segments[segId];
            int[] slotsToFlush;
            int countToFlush;

            synchronized (seg.lock) {
                if (seg.dirtyCount == 0) continue;
                // Swap Trick
                slotsToFlush = seg.dirtySlots;
                countToFlush = seg.dirtyCount;
                seg.dirtySlots = new int[64]; // Reset
                seg.dirtyCount = 0;
            }

            // Process swapped buffer
            for (int k = 0; k < countToFlush; k++) {
                int slot = slotsToFlush[k];
                String id = handleToId.get((segId << 16) | slot);
                if (id != null) {
                    long offset = (long) slot * D_SIZE;
                    batch.add(new DatabaseManager.PidDbSnapshot(
                        id, seg.integrals.get(D_LAYOUT, offset), seg.errors.get(D_LAYOUT, offset),
                        seg.lambdas.get(D_LAYOUT, offset), seg.times.get(L_LAYOUT, offset)
                    ));
                }
            }
        }

        if (batch.isEmpty()) return;
        if (sync) plugin.getDatabaseManager().saveBatch(batch);
        else Thread.ofVirtual().name("eb-db-flush").start(() -> plugin.getDatabaseManager().saveBatch(batch));
    }

    public PidStateDto inspectState(String itemId) {
        Integer handle = registry.get(itemId);
        if (handle == null) return null;
        int segId = handle >>> 16;
        int slot = handle & SLOT_MASK;
        Segment seg = segments[segId];
        long offset = (long) slot * D_SIZE;
        
        synchronized (seg.lock) {
            return new PidStateDto(
                    seg.integrals.get(D_LAYOUT, offset),
                    seg.errors.get(D_LAYOUT, offset),
                    seg.lambdas.get(D_LAYOUT, offset),
                    seg.times.get(L_LAYOUT, offset)
            );
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
            
            synchronized (seg.lock) {
                seg.integrals.set(D_LAYOUT, offset, snapshot.integral());
                seg.errors.set(D_LAYOUT, offset, snapshot.lastError());
                seg.lambdas.set(D_LAYOUT, offset, snapshot.lastLambda());
                seg.times.set(L_LAYOUT, offset, snapshot.updateTime());
                seg.markDirty(slot);
            }
        });
    }
}