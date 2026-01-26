package top.ellan.ecobridge;

import jdk.incubator.vector.*;

import java.lang.foreign.*;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;

public class PidController {

    // =================================================================================
    // SIMD 配置: 根据 CPU 自动选择最佳向量位宽 (128/256/512 bit)
    // =================================================================================
    private static final VectorSpecies<Double> SPECIES = DoubleVector.SPECIES_PREFERRED;

    // FFM 内存布局
    private static final GroupLayout PID_STATE_LAYOUT = MemoryLayout.structLayout(
        ValueLayout.JAVA_DOUBLE.withName("integral"),
        ValueLayout.JAVA_DOUBLE.withName("lastError"),
        ValueLayout.JAVA_DOUBLE.withName("lastLambda"),
        ValueLayout.JAVA_LONG.withName("updateTime"),
        ValueLayout.JAVA_BYTE.withName("isDirty"),
        MemoryLayout.paddingLayout(7)
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
    private final ConcurrentHashMap<String, Long> offsetMap = new ConcurrentHashMap<>(4096);
    private final Queue<Long> dirtyOffsetQueue = new ConcurrentLinkedQueue<>();
    private final MemorySegment memorySegment;
    private final ReentrantLock allocationLock = new ReentrantLock();
    private long currentOffset = 0;

    // PID 常量 (标量)
    private static final double PID_TAU_INV = 0.00001929;
    private static final double TARGET_VOL = 1000.0;
    private static final double BASE_LAMBDA = 0.002;
    private static final double KP = 0.00001;
    // [修复] 补全缺失的常量
    private static final double KP_NEG_FACTOR = 0.6; 
    private static final double KI = 0.000001;
    private static final double KD = 0.00005;
    private static final double TARGET_DEADBAND = TARGET_VOL * 0.02;

    public PidController(EcoBridge plugin) {
        this.plugin = plugin;
        // 预分配 32MB 堆外内存
        this.memorySegment = arena.allocate(32 * 1024 * 1024, 8); 
    }

    private long getOrAllocateOffset(String itemId, long now) {
        return offsetMap.computeIfAbsent(itemId, k -> {
            allocationLock.lock();
            try {
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
     */
    public void calculateBatch(List<String> itemIds, double[] volumes) {
        int count = itemIds.size();
        long now = System.currentTimeMillis();
        
        // 1. 准备数据数组 (Structure of Arrays)
        double[] integrals = new double[count];
        double[] lastErrors = new double[count];
        double[] lastLambdas = new double[count];
        long[] updateTimes = new long[count];
        long[] offsets = new long[count];

        // 2. Gather: 从堆外内存收集数据
        for (int i = 0; i < count; i++) {
            offsets[i] = getOrAllocateOffset(itemIds.get(i), now);
            MemorySegment slice = memorySegment.asSlice(offsets[i], STATE_SIZE);
            integrals[i] = (double) VH_INTEGRAL.get(slice, 0L);
            lastErrors[i] = (double) VH_LAST_ERROR.get(slice, 0L);
            lastLambdas[i] = (double) VH_LAST_LAMBDA.get(slice, 0L);
            updateTimes[i] = (long) VH_UPDATE_TIME.get(slice, 0L);
        }

        // 3. SIMD Compute Loop
        int i = 0;
        int loopBound = SPECIES.loopBound(count);

        for (; i < loopBound; i += SPECIES.length()) {
            // --- Load Vectors ---
            var vIntegral = DoubleVector.fromArray(SPECIES, integrals, i);
            var vLastError = DoubleVector.fromArray(SPECIES, lastErrors, i);
            var vLastLambda = DoubleVector.fromArray(SPECIES, lastLambdas, i);
            var vVolume = DoubleVector.fromArray(SPECIES, volumes, i);
            
            // 处理 UpdateTime
            double[] dtTemp = new double[SPECIES.length()];
            for(int j=0; j<SPECIES.length(); j++) dtTemp[j] = (now - updateTimes[i+j]) * 0.001;
            var vDt = DoubleVector.fromArray(SPECIES, dtTemp, 0);

            // --- Vector Math Logic ---
            
            // 1. Clamp DT
            vDt = vDt.min(1.0).max(0.05);

            // 2. Decay: 近似 exp (-dt * TAU) => 1 - dt * TAU
            var vDecay = vDt.mul(PID_TAU_INV).neg().add(1.0); 
            var vNewIntegral = vIntegral.mul(vDecay);

            // 3. Error
            var vError = vVolume.sub(TARGET_VOL);
            
            // 4. Deadband (Masking)
            var maskDeadband = vError.abs().compare(VectorOperators.LT, TARGET_DEADBAND);
            vError = vError.blend(0.0, maskDeadband);

            // 5. Asymmetric P
            var maskNeg = vError.compare(VectorOperators.LT, 0.0);
            // [修复] blend 方法第二个参数必须是 Vector (或者标量广播后)，不能直接传 double
            var vKpBase = DoubleVector.broadcast(SPECIES, KP);
            var vKpNeg = DoubleVector.broadcast(SPECIES, KP * KP_NEG_FACTOR);
            var vKp = vKpBase.blend(vKpNeg, maskNeg);
            
            var vP = vError.mul(vKp);

            // 6. Integral Accumulation & Clamping
            vNewIntegral = vNewIntegral.add(vError.mul(vDt));
            vNewIntegral = vNewIntegral.min(30000.0).max(-30000.0);

            // 7. Derivative
            var vD = vError.sub(vLastError).div(vDt).mul(KD);

            // 8. Output Lambda
            var vRaw = vP.add(vNewIntegral.mul(KI)).add(vD).add(BASE_LAMBDA);
            // Exponential Smoothing
            var vLambda = vRaw.mul(0.05).add(vLastLambda.mul(0.95));
            // Clamping
            vLambda = vLambda.min(0.01).max(0.0005);

            // --- Store Results Back to Arrays ---
            vNewIntegral.intoArray(integrals, i);
            vError.intoArray(lastErrors, i);
            vLambda.intoArray(lastLambdas, i);
        }

        // 4. 处理剩余元素 (Tail Loop - Scalar fallback)
        for (; i < count; i++) {
            double dt = (now - updateTimes[i]) * 0.001;
            if (dt > 1.0) dt = 1.0; else if (dt < 0.05) dt = 0.05;
            double decay = Math.exp(-dt * PID_TAU_INV);
            double integral = integrals[i] * decay;
            double error = volumes[i] - TARGET_VOL;
            if (Math.abs(error) < TARGET_DEADBAND) error = 0.0;
            // [修复] 补全 KP_NEG_FACTOR 引用
            double kpEff = (error < 0) ? KP * KP_NEG_FACTOR : KP;
            integral += error * dt;
            integral = Math.max(-30000, Math.min(30000, integral));
            double d = KD * ((error - lastErrors[i]) / dt);
            double raw = BASE_LAMBDA + kpEff * error + KI * integral + d;
            double lambda = raw * 0.05 + lastLambdas[i] * 0.95;
            lastLambdas[i] = Math.max(0.0005, Math.min(0.01, lambda));
            integrals[i] = integral;
            lastErrors[i] = error;
        }

        // 5. Scatter: 将结果回写到堆外内存
        for (int k = 0; k < count; k++) {
            MemorySegment slice = memorySegment.asSlice(offsets[k], STATE_SIZE);
            VH_INTEGRAL.set(slice, 0L, integrals[k]);
            VH_LAST_ERROR.set(slice, 0L, lastErrors[k]);
            VH_LAST_LAMBDA.set(slice, 0L, lastLambdas[k]);
            VH_UPDATE_TIME.set(slice, 0L, now);
            
            byte isDirty = (byte) VH_IS_DIRTY.get(slice, 0L);
            if (isDirty == 0) {
                VH_IS_DIRTY.set(slice, 0L, (byte) 1);
                dirtyOffsetQueue.offer(offsets[k]);
            }
        }
    }

    // 保持原有接口兼容
    public double getCachedResult(String id) {
        Long offset = offsetMap.get(id);
        if (offset == null) return BASE_LAMBDA;
        return (double) VH_LAST_LAMBDA.get(memorySegment, offset);
    }

    public void flushBuffer(boolean sync) {
        if (dirtyOffsetQueue.isEmpty()) return;

        List<DatabaseManager.PidDbSnapshot> batch = new ArrayList<>(Math.min(dirtyOffsetQueue.size(), 500));
        Long offset;
        int limit = 1000;
        
        while (limit-- > 0 && (offset = dirtyOffsetQueue.poll()) != null) {
            MemorySegment slice = memorySegment.asSlice(offset, STATE_SIZE);
            VH_IS_DIRTY.set(slice, 0L, (byte) 0);
            
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

        if (batch.isEmpty()) return;
        Runnable task = () -> plugin.getDatabaseManager().saveBatch(batch);
        
        if (sync) task.run();
        else Thread.ofVirtual().start(task);
    }
    
    private String findItemIdByOffset(long targetOffset) {
        for (var entry : offsetMap.entrySet()) {
            if (entry.getValue() == targetOffset) return entry.getKey();
        }
        return null;
    }

    public void loadAllStates() {
        plugin.getDatabaseManager().loadStates(snapshot -> {
            long offset = getOrAllocateOffset(snapshot.itemId(), snapshot.updateTime());
            MemorySegment slice = memorySegment.asSlice(offset, STATE_SIZE);
            VH_INTEGRAL.set(slice, 0L, snapshot.integral());
            VH_LAST_ERROR.set(slice, 0L, snapshot.lastError());
            VH_LAST_LAMBDA.set(slice, 0L, snapshot.lastLambda());
        });
    }
    
    public void close() {
        if (arena.scope().isAlive()) arena.close();
    }
}