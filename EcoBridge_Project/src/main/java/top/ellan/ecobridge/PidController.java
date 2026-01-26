package top.ellan.ecobridge;

import jdk.incubator.vector.*;
import java.lang.foreign.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.StampedLock;

/**
 * PidController (Ultimate Optimized Edition)
 * 
 * 核心优化:
 * 1. Handle 机制: 移除热点路径的 Map 查找，改用 int 句柄直接定位。
 * 2. Zero-GC: ThreadLocal 上下文缓冲 + 自动扩容数组。
 * 3. Data Swizzling: 计算前将离散数据重排为连续内存，最大化缓存命中。
 * 4. FMA & ILP: 使用融合乘加指令和指令级并行掩盖除法延迟。
 * 5. False Sharing: 显式 Cache Line 填充。
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
    private static final long D_SIZE = 8L; // double size

    // --- 预计算向量常量 (Broadcast) ---
    private static final DoubleVector V_TARGET     = DoubleVector.broadcast(SPECIES, 1000.0);
    private static final DoubleVector V_DEADBAND   = DoubleVector.broadcast(SPECIES, 20.0);
    
    // PID 参数
    // [Fix] 移除了未使用的 V_TAU_INV，改用负倒数进行 FMA 计算
    private static final DoubleVector V_NEG_TAU_INV = DoubleVector.broadcast(SPECIES, -0.00001929);
    
    private static final DoubleVector V_KP         = DoubleVector.broadcast(SPECIES, 0.00001);
    private static final DoubleVector V_KP_NEG     = DoubleVector.broadcast(SPECIES, 0.00001 * 0.6);
    private static final DoubleVector V_KI         = DoubleVector.broadcast(SPECIES, 0.000001);
    private static final DoubleVector V_KD         = DoubleVector.broadcast(SPECIES, 0.00005);
    private static final DoubleVector V_BASE       = DoubleVector.broadcast(SPECIES, 0.002);
    
    private static final DoubleVector V_ALPHA      = DoubleVector.broadcast(SPECIES, 0.05);
    private static final DoubleVector V_BETA       = DoubleVector.broadcast(SPECIES, 0.95);
    
    private static final DoubleVector V_ZERO       = DoubleVector.broadcast(SPECIES, 0.0);
    private static final DoubleVector V_ONE        = DoubleVector.broadcast(SPECIES, 1.0);
    
    // 限制范围
    private static final DoubleVector V_I_MAX      = DoubleVector.broadcast(SPECIES, 30000.0);
    private static final DoubleVector V_I_MIN      = DoubleVector.broadcast(SPECIES, -30000.0);
    private static final DoubleVector V_L_MAX      = DoubleVector.broadcast(SPECIES, 0.01);
    private static final DoubleVector V_L_MIN      = DoubleVector.broadcast(SPECIES, 0.0005);
    private static final DoubleVector V_DT_MAX     = DoubleVector.broadcast(SPECIES, 1.0);
    private static final DoubleVector V_DT_MIN     = DoubleVector.broadcast(SPECIES, 0.05);

    // ==================== 2. 核心成员 ====================

    private final EcoBridge plugin;
    private final Arena arena;
    private final Segment[] segments;
    // 仅用于冷启动注册 (热点路径不访问)
    private final ConcurrentHashMap<String, Integer> registry = new ConcurrentHashMap<>(4096);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    // ThreadLocal 上下文 (Zero-GC 核心)
    private final ThreadLocal<CalcContext> tlContext = ThreadLocal.withInitial(CalcContext::new);

    // [Fix] 构造函数名必须与类名 PidController 一致
    public PidController(EcoBridge plugin) {
        this.plugin = plugin;
        this.arena = Arena.ofShared();
        this.segments = new Segment[SEGMENT_COUNT];
        for (int i = 0; i < SEGMENT_COUNT; i++) {
            this.segments[i] = new Segment(arena);
        }
        plugin.getLogger().info("[PID] Ultimate Optimized Controller Initialized. Vector Lane: " + V_LEN);
    }

    // ==================== 3. 注册 API (Cold Path) ====================

    /**
     * 获取实体的唯一 Handle (int)。
     * 调用方必须缓存此 Handle，后续计算直接传入 int。
     * Handle 结构: [高16位: SegmentId] [低16位: SlotIndex]
     */
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
                    return -1; // 或抛出异常
                }
                return (segId << 16) | slot;
            } finally {
                seg.lock.unlockWrite(stamp);
            }
        });
    }

    public double getLambdaByString(String itemId) {
        // 1. 从注册表中查找 Handle (非阻塞)
        Integer handle = registry.get(itemId);
        if (handle == null) return 0.002; // 默认基准值
        
        // 2. 解析 Handle
        int segId = handle >>> 16;
        int slot = handle & SLOT_MASK;
        
        // 3. 定位内存段
        Segment seg = segments[segId];
        long offset = (long) slot * D_SIZE;
        
        // 4. StampedLock 乐观读 (无锁极速读取)
        long stamp = seg.lock.tryOptimisticRead();
        double lambda = seg.lambdas.get(D_LAYOUT, offset);
        
        // 5. 如果读取期间发生了写入，则升级为读锁重试
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

    // ==================== 4. 批量计算 API (Hot Path) ====================

    /**
     * 极速批量计算入口
     * @param handles 实体的 handle 数组 (预先通过 getHandle 获取)
     * @param volumes 当前体积数组
     * @param count 有效数据长度
     */
    public void calculateBatch(int[] handles, double[] volumes, int count) {
        if (closed.get() || count == 0) return;
        
        CalcContext ctx = tlContext.get();
        ctx.reset();
        
        // --- Phase 1: Data Swizzling (数据重排) ---
        // 将乱序的输入按 Segment 归类到 ThreadLocal 连续缓冲区
        // 这极大地提升了内存局部性，并减少了锁的获取次数
        for (int i = 0; i < count; i++) {
            int h = handles[i];
            if (h == -1) continue; // 跳过无效句柄
            int segId = h >>> 16;
            int slot = h & SLOT_MASK;
            ctx.push(segId, slot, volumes[i]);
        }

        long now = System.currentTimeMillis();

        // --- Phase 2: Process Segments (串行遍历段) ---
        // 避免了多线程上下文切换的开销，对于这种极快计算，串行批处理通常更快
        for (int i = 0; i < SEGMENT_COUNT; i++) {
            if (ctx.counts[i] > 0) {
                processSegment(segments[i], ctx, i, now);
            }
        }
    }

    // ==================== 5. 核心计算逻辑 (SIMD + FMA + ILP) ====================

    private void processSegment(Segment seg, CalcContext ctx, int segId, long now) {
        int count = ctx.counts[segId];
        int[] slots = ctx.segSlots[segId];
        double[] vols = ctx.segVols[segId];
        
        long stamp = seg.lock.writeLock();
        try {
            // 直接获取 MemorySegment 引用，避免循环内解引用
            MemorySegment msInt = seg.integrals;
            MemorySegment msErr = seg.errors;
            MemorySegment msLam = seg.lambdas;
            MemorySegment msTime = seg.times;
            MemorySegment msDirty = seg.dirty;

            int i = 0;
            int loopBound = SPECIES.loopBound(count);
            double[] vBuf = ctx.vBufState; // L1 Cache Hot Buffer

            // --- SIMD Loop ---
            for (; i < loopBound; i += V_LEN) {
                
                // 1. Manual Gather (手动收集)
                // 从 SoA 的离散位置加载数据到连续的 L1 缓冲
                for (int lane = 0; lane < V_LEN; lane++) {
                    int slotIdx = slots[i + lane];
                    long offset = (long) slotIdx * D_SIZE;
                    
                    // 使用 FFM 直接访问，JDK 22+ JIT 会优化为极快的内存偏移指令
                    vBuf[lane]             = msInt.get(D_LAYOUT, offset);       // Integral
                    vBuf[lane + V_LEN]     = msErr.get(D_LAYOUT, offset);       // Last Err
                    vBuf[lane + 2 * V_LEN] = msLam.get(D_LAYOUT, offset);       // Lambda
                    long lastT             = msTime.get(L_LAYOUT, offset);
                    vBuf[lane + 3 * V_LEN] = (now - lastT) * 0.001;             // dt (s)
                }

                // Load to Vectors
                DoubleVector vIntegral = DoubleVector.fromArray(SPECIES, vBuf, 0);
                DoubleVector vLastErr  = DoubleVector.fromArray(SPECIES, vBuf, V_LEN);
                DoubleVector vLambda   = DoubleVector.fromArray(SPECIES, vBuf, 2 * V_LEN);
                DoubleVector vDt       = DoubleVector.fromArray(SPECIES, vBuf, 3 * V_LEN);
                DoubleVector vVol      = DoubleVector.fromArray(SPECIES, vols, i);

                // --- Extreme Optimization Logic ---

                // [Step 1] Clamp DT
                vDt = vDt.max(V_DT_MIN).min(V_DT_MAX);

                // [Step 2] Error Calculation
                DoubleVector vErrRaw = vVol.sub(V_TARGET);
                // Branchless Deadband (无分支死区处理)
                VectorMask<Double> maskDead = vErrRaw.abs().compare(VectorOperators.LT, V_DEADBAND);
                DoubleVector vErr = vErrRaw.blend(V_ZERO, maskDead);

                // [Step 3 - ILP Trick] 提前发射除法指令 (D Term)
                // 除法耗时最长(约15-20 cycles)，先启动它，利用 CPU 乱序执行掩盖延迟
                DoubleVector vD_Part = vErr.sub(vLastErr).div(vDt);

                // [Step 4 - FMA] Integral Decay
                // Formula: decay = 1.0 + (dt * -tau_inv)
                // 使用 FMA 一步完成，无中间舍入误差，且比 mul + add 快
                DoubleVector vDecay = vDt.fma(V_NEG_TAU_INV, V_ONE);

                // [Step 5 - FMA] Integral Accumulate
                // I = I * decay
                vIntegral = vIntegral.mul(vDecay);
                // I = I + (err * dt) -> I = err.fma(dt, I)
                // 再次利用 FMA
                vIntegral = vErr.fma(vDt, vIntegral);
                
                vIntegral = vIntegral.max(V_I_MIN).min(V_I_MAX);

                // [Step 6] P Term (Branchless)
                VectorMask<Double> maskNeg = vErr.compare(VectorOperators.LT, V_ZERO);
                DoubleVector vKp = V_KP.blend(V_KP_NEG, maskNeg);
                DoubleVector vP = vErr.mul(vKp);

                // [Step 7] Finish D Term & I Term
                DoubleVector vD = vD_Part.mul(V_KD);
                DoubleVector vI = vIntegral.mul(V_KI);

                // [Step 8] Raw Lambda Sum
                DoubleVector vRaw = vP.add(vI).add(vD).add(V_BASE);

                // [Step 9 - FMA] Lambda Update (Ultimate Optimization)
                // lambda = lambda * beta + raw * alpha
                // 融合为: lambda.fma(beta, raw * alpha)
                vLambda = vLambda.fma(V_BETA, vRaw.mul(V_ALPHA));
                
                vLambda = vLambda.max(V_L_MIN).min(V_L_MAX);

                // --- 2. Manual Scatter (手动写回) ---
                
                vIntegral.intoArray(vBuf, 0);
                vErr.intoArray(vBuf, V_LEN); // 更新 LastError 为当前 Err
                vLambda.intoArray(vBuf, 2 * V_LEN);

                for (int lane = 0; lane < V_LEN; lane++) {
                    int slotIdx = slots[i + lane];
                    long offset = (long) slotIdx * D_SIZE;

                    msInt.set(D_LAYOUT, offset, vBuf[lane]);
                    msErr.set(D_LAYOUT, offset, vBuf[lane + V_LEN]); // Write new error as last error
                    msLam.set(D_LAYOUT, offset, vBuf[lane + 2 * V_LEN]);
                    msTime.set(L_LAYOUT, offset, now);
                    msDirty.set(B_LAYOUT, (long) slotIdx, (byte) 1);
                }
            }

            // --- Scalar Tail (处理剩余不足 V_LEN 的部分) ---
            for (; i < count; i++) {
                int slotIdx = slots[i];
                long offset = (long) slotIdx * D_SIZE;
                
                // 标量逻辑 (JIT 会自动内联)
                double dt = (now - msTime.get(L_LAYOUT, offset)) * 0.001;
                dt = Math.max(0.05, Math.min(1.0, dt));

                double integral = msInt.get(D_LAYOUT, offset);
                // I *= (1 - dt * tau)
                integral *= (1.0 - dt * 0.00001929);
                
                double vol = vols[i];
                double err = vol - 1000.0;
                if (Math.abs(err) < 20.0) err = 0.0;

                double kp = (err < 0) ? 0.000006 : 0.00001;
                
                // Scalar FMA (since JDK 9)
                integral = Math.fma(err, dt, integral);
                integral = Math.max(-30000, Math.min(30000, integral));
                
                double lastErr = msErr.get(D_LAYOUT, offset);
                double d = (err - lastErr) / dt * 0.00005;
                
                double raw = kp * err + integral * 0.000001 + d + 0.002;
                double lambda = msLam.get(D_LAYOUT, offset);
                
                // Scalar FMA
                lambda = Math.fma(lambda, 0.95, raw * 0.05);
                
                lambda = Math.max(0.0005, Math.min(0.01, lambda));

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

    // ==================== 6. 数据结构定义 ====================

    /**
     * 内存段容器 - 包含 Padding 以防止 False Sharing
     */
    private static class Segment {
        // [Fix] 添加 SuppressWarnings 消除 IDE 警告
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
            // 初始化默认值
            lambdas.setAtIndex(D_LAYOUT, slot, 0.002);
            times.setAtIndex(L_LAYOUT, slot, System.currentTimeMillis());
            return slot;
        }
        
        void warmup() {
            // 触发 OS 物理页分配 (Page Fault)
            for(int i=0; i<CAPACITY; i+=512) integrals.setAtIndex(D_LAYOUT, i, 0);
        }
    }

    /**
     * 线程本地工作区 - 包含自动扩容逻辑
     */
    private static class CalcContext {
        static final int INITIAL_CAPACITY = 512;
        static final int MAX_CAPACITY = 1024 * 1024;

        // 分段桶：segSlots[segId][idx]
        int[][] segSlots = new int[SEGMENT_COUNT][INITIAL_CAPACITY];
        double[][] segVols = new double[SEGMENT_COUNT][INITIAL_CAPACITY];
        final int[] counts = new int[SEGMENT_COUNT];

        // SIMD 交换缓冲区 (4 lanes * V_LEN)
        final double[] vBufState = new double[4 * V_LEN];

        void reset() {
            Arrays.fill(counts, 0);
        }

        void push(int segId, int slot, double vol) {
            int idx = counts[segId]++;
            
            // 自动扩容 (Geometric Expansion)
            if (idx >= segSlots[segId].length) {
                grow(segId);
            }
            
            segSlots[segId][idx] = slot;
            segVols[segId][idx] = vol;
        }
        
        private void grow(int segId) {
            int oldCap = segSlots[segId].length;
            if (oldCap >= MAX_CAPACITY) throw new OutOfMemoryError("PID Context buffer limit");
            
            int newCap = oldCap + (oldCap >> 1); // 1.5x
            if (newCap > MAX_CAPACITY) newCap = MAX_CAPACITY;
            
            int[] newSlots = new int[newCap];
            double[] newVols = new double[newCap];
            
            System.arraycopy(segSlots[segId], 0, newSlots, 0, oldCap);
            System.arraycopy(segVols[segId], 0, newVols, 0, oldCap);
            
            segSlots[segId] = newSlots;
            segVols[segId] = newVols;
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            if (arena.scope().isAlive()) arena.close();
        }
    }

// ==================== 诊断与监控 API (给指令层调用) ====================

    /**
     * 获取当前已注册的商品总数
     */
    public int getCacheSize() {
        return registry.size();
    }

    /**
     * 获取待写入数据库的“脏”数据总量
     */
    public int getDirtyQueueSize() {
        int totalDirty = 0;
        for (Segment seg : segments) {
            // 这里为了诊断准确性，使用 StampedLock
            long stamp = seg.lock.readLock();
            try {
                // 遍历 BitSet 中已分配的槽位，检查 dirty 标记
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

    /**
     * 强行刷写缓冲区：将所有标记为 Dirty 的数据存入数据库
     * 对应 /eb save 指令
     */
    public void flushBuffer(boolean sync) {
        if (closed.get()) return;

        List<DatabaseManager.PidDbSnapshot> batch = new ArrayList<>();

        for (int segId = 0; segId < SEGMENT_COUNT; segId++) {
            Segment seg = segments[segId];
            long stamp = seg.lock.writeLock();
            try {
                for (int i = seg.allocationMap.nextSetBit(0); i >= 0; i = seg.allocationMap.nextSetBit(i + 1)) {
                    if (seg.dirty.get(B_LAYOUT, (long) i) == (byte) 1) {
                        // 清除脏标记
                        seg.dirty.set(B_LAYOUT, (long) i, (byte) 0);

                        // 反查 ID (这是一个昂贵的操作，但仅在手动保存或插件关闭时触发)
                        int targetHandle = (segId << 16) | i;
                        String itemId = findIdByHandle(targetHandle);

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

        // 提交至数据库管理器
        if (sync) {
            plugin.getDatabaseManager().saveBatch(batch);
        } else {
            Thread.ofVirtual().start(() -> plugin.getDatabaseManager().saveBatch(batch));
        }
    }

    /**
     * 查看特定物品的 PID 内部状态
     * 对应 /eb inspect 指令
     */
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

    // 内部反查方法
    private String findIdByHandle(int handle) {
        for (var entry : registry.entrySet()) {
            if (entry.getValue() == handle) return entry.getKey();
        }
        return null;
    }

    // 定义数据传输对象 (DTO)
    public record PidStateDto(double integral, double lastError, double lastLambda, long updateTime) {}

public void loadAllStates() {
        if (closed.get()) return;

        // 调用 DatabaseManager 的异步流式读取
        plugin.getDatabaseManager().loadStates(snapshot -> {
            // 1. 为物品分配或获取 Handle (句柄)
            int handle = getHandle(snapshot.itemId());
            if (handle < 0) return;

            // 2. 解析句柄定位段和槽位
            int segId = handle >>> 16;
            int slot = handle & SLOT_MASK;
            Segment seg = segments[segId];
            long offset = (long) slot * D_SIZE;

            // 3. 使用写锁确保初始数据注入安全
            long stamp = seg.lock.writeLock();
            try {
                seg.integrals.set(D_LAYOUT, offset, snapshot.integral());
                seg.errors.set(D_LAYOUT, offset, snapshot.lastError());
                seg.lambdas.set(D_LAYOUT, offset, snapshot.lastLambda());
                seg.times.set(L_LAYOUT, offset, snapshot.updateTime());
                // 初始加载不需要标记为脏数据
                seg.dirty.set(B_LAYOUT, (long) slot, (byte) 0);
            } finally {
                seg.lock.unlockWrite(stamp);
            }
        });
    }
}