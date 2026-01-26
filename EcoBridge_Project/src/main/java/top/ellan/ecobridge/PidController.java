package top.ellan.ecobridge;

import jdk.incubator.vector.*;

import java.lang.foreign.*;
import java.lang.invoke.VarHandle;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PidController {

    // =================================================================================
    // SIMD 配置
    // =================================================================================
    private static final VectorSpecies<Double> SPECIES = DoubleVector.SPECIES_PREFERRED;

    // FFM 内存布局 (40 bytes per item)
    private static final GroupLayout PID_STATE_LAYOUT = MemoryLayout.structLayout(
        ValueLayout.JAVA_DOUBLE.withName("integral"),   // 0-7
        ValueLayout.JAVA_DOUBLE.withName("lastError"),  // 8-15
        ValueLayout.JAVA_DOUBLE.withName("lastLambda"), // 16-23
        ValueLayout.JAVA_LONG.withName("updateTime"),   // 24-31
        ValueLayout.JAVA_BYTE.withName("isDirty"),      // 32
        MemoryLayout.paddingLayout(7)                   // 33-39 (Align to 8 bytes)
    );

    private static final long STATE_SIZE = PID_STATE_LAYOUT.byteSize();
    
    // VarHandles
    private static final VarHandle VH_INTEGRAL = PID_STATE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("integral"));
    private static final VarHandle VH_LAST_ERROR = PID_STATE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("lastError"));
    private static final VarHandle VH_LAST_LAMBDA = PID_STATE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("lastLambda"));
    private static final VarHandle VH_UPDATE_TIME = PID_STATE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("updateTime"));
    private static final VarHandle VH_IS_DIRTY = PID_STATE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("isDirty"));

    // =================================================================================

    private final EcoBridge plugin;
    private final Arena arena = Arena.ofShared();
    private final MemorySegment memorySegment;
    
    // 索引管理
    private final ConcurrentHashMap<String, Long> offsetMap = new ConcurrentHashMap<>(4096);
    private final Queue<Long> dirtyOffsetQueue = new ConcurrentLinkedQueue<>();
    
    // 锁机制
    private final ReentrantReadWriteLock segmentLock = new ReentrantReadWriteLock();
    private final ReentrantLock allocationLock = new ReentrantLock();
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private long currentOffset = 0;

    // PID 常量
    private static final double PID_TAU_INV = 0.00001929;
    private static final double TARGET_VOL = 1000.0;
    private static final double BASE_LAMBDA = 0.002;
    private static final double KP = 0.00001;
    private static final double KP_NEG_FACTOR = 0.6;
    private static final double KI = 0.000001;
    private static final double KD = 0.00005;
    private static final double TARGET_DEADBAND = TARGET_VOL * 0.02;

    // [Zero-GC] ThreadLocal 缓存池，避免每分钟创建大数组
    private static final ThreadLocal<Buffers> TL_BUFFERS = ThreadLocal.withInitial(Buffers::new);

    private static class Buffers {
        double[] integrals = new double[1024];
        double[] lastErrors = new double[1024];
        double[] lastLambdas = new double[1024];
        long[] updateTimes = new long[1024];
        long[] offsets = new long[1024];

        void ensureCapacity(int size) {
            if (integrals.length < size) {
                int newSize = Math.max(integrals.length * 2, size);
                integrals = new double[newSize];
                lastErrors = new double[newSize];
                lastLambdas = new double[newSize];
                updateTimes = new long[newSize];
                offsets = new long[newSize];
            }
        }
    }

    public PidController(EcoBridge plugin) {
        this.plugin = plugin;
        // 预分配 32MB 堆外内存 (可容纳约 80万个物品状态)
        this.memorySegment = arena.allocate(32 * 1024 * 1024, 8); 
    }

    private long getOrAllocateOffset(String itemId, long now) {
        return offsetMap.computeIfAbsent(itemId, k -> {
            allocationLock.lock();
            try {
                if (isClosed.get()) throw new IllegalStateException("Controller closed");
                long offset = currentOffset;
                if (offset + STATE_SIZE > memorySegment.byteSize()) throw new OutOfMemoryError("Off-heap full!");
                currentOffset += STATE_SIZE;
                
                MemorySegment slice = memorySegment.asSlice(offset, STATE_SIZE);
                VH_LAST_LAMBDA.set(slice, 0L, BASE_LAMBDA);
                VH_UPDATE_TIME.set(slice, 0L, now - 1000);
                return offset;
            } finally {
                allocationLock.unlock();
            }
        });
    }

    /**
     * [SIMD 核心] 批量向量化计算
     * 线程安全：加写锁，防止与 flushBuffer 冲突
     */
    public void calculateBatch(List<String> itemIds, double[] volumes) {
        if (isClosed.get()) return;
        
        // 1. 获取线程本地缓存，避免 GC
        Buffers buf = TL_BUFFERS.get();
        int count = itemIds.size();
        buf.ensureCapacity(count);
        
        long now = System.currentTimeMillis();

        // 加写锁：计算过程中通过 VarHandle 写入内存，不允许同时发生数据库刷写
        segmentLock.writeLock().lock();
        try {
            // 2. Gather: 从堆外内存收集数据到数组
            for (int i = 0; i < count; i++) {
                long offset = getOrAllocateOffset(itemIds.get(i), now);
                buf.offsets[i] = offset;
                
                MemorySegment slice = memorySegment.asSlice(offset, STATE_SIZE);
                buf.integrals[i] = (double) VH_INTEGRAL.get(slice, 0L);
                buf.lastErrors[i] = (double) VH_LAST_ERROR.get(slice, 0L);
                buf.lastLambdas[i] = (double) VH_LAST_LAMBDA.get(slice, 0L);
                buf.updateTimes[i] = (long) VH_UPDATE_TIME.get(slice, 0L);
            }

            // 3. SIMD Compute Loop
            int i = 0;
            int loopBound = SPECIES.loopBound(count);

            for (; i < loopBound; i += SPECIES.length()) {
                var vIntegral = DoubleVector.fromArray(SPECIES, buf.integrals, i);
                var vLastError = DoubleVector.fromArray(SPECIES, buf.lastErrors, i);
                var vLastLambda = DoubleVector.fromArray(SPECIES, buf.lastLambdas, i);
                var vVolume = DoubleVector.fromArray(SPECIES, volumes, i);
                
                // dt 计算
                double[] dtTemp = new double[SPECIES.length()];
                for(int j=0; j<SPECIES.length(); j++) dtTemp[j] = (now - buf.updateTimes[i+j]) * 0.001;
                var vDt = DoubleVector.fromArray(SPECIES, dtTemp, 0);

                // --- Math ---
                vDt = vDt.min(1.0).max(0.05); // Clamp DT

                var vDecay = vDt.mul(PID_TAU_INV).neg().add(1.0); // 1 - dt * tau
                var vNewIntegral = vIntegral.mul(vDecay);

                var vError = vVolume.sub(TARGET_VOL);
                
                // Deadband
                var maskDeadband = vError.abs().compare(VectorOperators.LT, TARGET_DEADBAND);
                vError = vError.blend(0.0, maskDeadband);

                // Asymmetric P
                var maskNeg = vError.compare(VectorOperators.LT, 0.0);
                var vKpBase = DoubleVector.broadcast(SPECIES, KP);
                var vKpNeg = DoubleVector.broadcast(SPECIES, KP * KP_NEG_FACTOR);
                var vKp = vKpBase.blend(vKpNeg, maskNeg);
                
                var vP = vError.mul(vKp);

                // Accumulate & Clamp
                vNewIntegral = vNewIntegral.add(vError.mul(vDt));
                vNewIntegral = vNewIntegral.min(30000.0).max(-30000.0);

                var vD = vError.sub(vLastError).div(vDt).mul(KD);

                var vRaw = vP.add(vNewIntegral.mul(KI)).add(vD).add(BASE_LAMBDA);
                var vLambda = vRaw.mul(0.05).add(vLastLambda.mul(0.95)); // Smooth
                vLambda = vLambda.min(0.01).max(0.0005); // Clamp Lambda

                // --- Store ---
                vNewIntegral.intoArray(buf.integrals, i);
                vError.intoArray(buf.lastErrors, i);
                vLambda.intoArray(buf.lastLambdas, i);
            }

            // 4. Tail Loop (Scalar fallback)
            for (; i < count; i++) {
                double dt = (now - buf.updateTimes[i]) * 0.001;
                if (dt > 1.0) dt = 1.0; else if (dt < 0.05) dt = 0.05;
                
                // 使用泰勒展开近似 Math.exp(-dt * TAU) -> 1 - dt * TAU，保持与 SIMD 逻辑一致性
                double decay = 1.0 - (dt * PID_TAU_INV);
                double integral = buf.integrals[i] * decay;
                double error = volumes[i] - TARGET_VOL;
                
                if (Math.abs(error) < TARGET_DEADBAND) error = 0.0;
                double kpEff = (error < 0) ? KP * KP_NEG_FACTOR : KP;
                
                integral += error * dt;
                integral = Math.max(-30000, Math.min(30000, integral));
                
                double d = KD * ((error - buf.lastErrors[i]) / dt);
                double raw = BASE_LAMBDA + kpEff * error + KI * integral + d;
                double lambda = raw * 0.05 + buf.lastLambdas[i] * 0.95;
                
                buf.lastLambdas[i] = Math.max(0.0005, Math.min(0.01, lambda));
                buf.integrals[i] = integral;
                buf.lastErrors[i] = error;
            }

            // 5. Scatter: 回写到堆外内存
            for (int k = 0; k < count; k++) {
                MemorySegment slice = memorySegment.asSlice(buf.offsets[k], STATE_SIZE);
                VH_INTEGRAL.set(slice, 0L, buf.integrals[k]);
                VH_LAST_ERROR.set(slice, 0L, buf.lastErrors[k]);
                VH_LAST_LAMBDA.set(slice, 0L, buf.lastLambdas[k]);
                VH_UPDATE_TIME.set(slice, 0L, now);
                
                // 原子操作：标记为脏数据
                byte isDirty = (byte) VH_IS_DIRTY.get(slice, 0L);
                if (isDirty == 0) {
                    VH_IS_DIRTY.set(slice, 0L, (byte) 1);
                    dirtyOffsetQueue.offer(buf.offsets[k]);
                }
            }
        } finally {
            segmentLock.writeLock().unlock();
        }
    }

    public double getCachedResult(String id) {
        Long offset = offsetMap.get(id);
        if (offset == null) return BASE_LAMBDA;
        
        // 读操作不需要全锁，因为 lambda 是 64 位原子写入的
        // 如果需要绝对一致性可以使用 readLock，但这里性能优先
        return (double) VH_LAST_LAMBDA.get(memorySegment, offset);
    }

    public void flushBuffer(boolean sync) {
        if (dirtyOffsetQueue.isEmpty() || isClosed.get()) return;

        // 构建快照列表
        List<DatabaseManager.PidDbSnapshot> batch = new java.util.ArrayList<>();
        Long offset;
        int limit = 1000; // 每次最多刷写 1000 条
        
        // 加写锁：防止在读取数据组装 batch 时，SIMD 线程修改了部分数据导致状态不一致
        // 虽然只读数据看似可以用读锁，但我们要执行 getAndSet 清除脏标记，这属于修改操作
        segmentLock.writeLock().lock();
        try {
            while (limit-- > 0 && (offset = dirtyOffsetQueue.poll()) != null) {
                MemorySegment slice = memorySegment.asSlice(offset, STATE_SIZE);
                
                // [关键修复] 原子性清除脏标记
                // 修复：使用 compareAndSet 或者直接 get + set 的组合
                // 由于 VarHandle.getAndSet 对于 MemorySegment 的调用语法是:
                // getAndSet(MemorySegment, long offset, byte newValue)
                byte prevDirty = (byte) VH_IS_DIRTY.getAndSet(slice, 0L, (byte) 0);
                
                if (prevDirty == 1) {
                    String itemId = findItemIdByOffset(offset); 
                    if (itemId == null) continue;

                    batch.add(new DatabaseManager.PidDbSnapshot(
                        itemId,
                        (double) VH_INTEGRAL.get(slice, 0L),
                        (double) VH_LAST_ERROR.get(slice, 0L),
                        (double) VH_LAST_LAMBDA.get(slice, 0L),
                        (long) VH_UPDATE_TIME.get(slice, 0L)
                    ));
                }
            }
        } catch (UnsupportedOperationException e) {
            // 如果 getAndSet 不支持，使用替代方案
            plugin.getLogger().warning("VarHandle.getAndSet not supported, using fallback method");
            
            // 清空队列并重新处理
            dirtyOffsetQueue.clear();
            
            // 重新扫描所有偏移量
            for (var entry : offsetMap.entrySet()) {
                long off = entry.getValue();
                MemorySegment slice = memorySegment.asSlice(off, STATE_SIZE);
                byte isDirty = (byte) VH_IS_DIRTY.get(slice, 0L);
                
                if (isDirty == 1) {
                    VH_IS_DIRTY.set(slice, 0L, (byte) 0);
                    
                    batch.add(new DatabaseManager.PidDbSnapshot(
                        entry.getKey(),
                        (double) VH_INTEGRAL.get(slice, 0L),
                        (double) VH_LAST_ERROR.get(slice, 0L),
                        (double) VH_LAST_LAMBDA.get(slice, 0L),
                        (long) VH_UPDATE_TIME.get(slice, 0L)
                    ));
                }
                
                if (batch.size() >= 1000) break;
            }
        } finally {
            segmentLock.writeLock().unlock();
        }

        if (batch.isEmpty()) return;
        
        // 数据库 IO 操作必须在锁外执行！否则会阻塞计算线程
        Runnable task = () -> plugin.getDatabaseManager().saveBatch(batch);
        
        if (sync) task.run();
        else Thread.ofVirtual().start(task);
    }
    
    private String findItemIdByOffset(long targetOffset) {
        // 反向查找 (性能较低，但仅在 flush 时调用，且 Map 为并发哈希，尚可接受)
        // 优化方案：在 Struct 中存储 ID 的 Hash 或增加反向索引，但为了省内存暂维持现状
        for (var entry : offsetMap.entrySet()) {
            if (entry.getValue() == targetOffset) return entry.getKey();
        }
        return null;
    }

    public void loadAllStates() {
        if (isClosed.get()) return;
        plugin.getDatabaseManager().loadStates(snapshot -> {
            segmentLock.writeLock().lock();
            try {
                long offset = getOrAllocateOffset(snapshot.itemId(), snapshot.updateTime());
                MemorySegment slice = memorySegment.asSlice(offset, STATE_SIZE);
                VH_INTEGRAL.set(slice, 0L, snapshot.integral());
                VH_LAST_ERROR.set(slice, 0L, snapshot.lastError());
                VH_LAST_LAMBDA.set(slice, 0L, snapshot.lastLambda());
                // 加载的数据默认为非脏数据 (isDirty = 0)
            } finally {
                segmentLock.writeLock().unlock();
            }
        });
    }
    
    // DTO & Inspect Methods
    public record PidStateDto(double integral, double lastError, double lastLambda, long updateTime) {}

    public PidStateDto inspectState(String itemId) {
        Long offset = offsetMap.get(itemId);
        if (offset == null) return null;
        
        segmentLock.readLock().lock(); // 加读锁，保证读取到一致的状态
        try {
            MemorySegment slice = memorySegment.asSlice(offset, STATE_SIZE);
            return new PidStateDto(
                (double) VH_INTEGRAL.get(slice, 0L),
                (double) VH_LAST_ERROR.get(slice, 0L),
                (double) VH_LAST_LAMBDA.get(slice, 0L),
                (long) VH_UPDATE_TIME.get(slice, 0L)
            );
        } finally {
            segmentLock.readLock().unlock();
        }
    }

    public int getDirtyQueueSize() {
        return dirtyOffsetQueue.size();
    }

    public int getCacheSize() {
        return offsetMap.size();
    }
    
    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            // 确保没有线程正在访问内存
            segmentLock.writeLock().lock();
            try {
                if (arena.scope().isAlive()) arena.close();
            } finally {
                segmentLock.writeLock().unlock();
            }
        }
    }
}