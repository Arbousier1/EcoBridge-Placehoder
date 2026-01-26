package top.ellan.ecobridge;

import jdk.incubator.vector.VectorSpecies;
import org.bukkit.Bukkit;

import java.lang.management.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * PerformanceMonitor - 性能监控和诊断
 * 
 * 功能:
 * 1. SIMD 向量化率统计
 * 2. GC 压力监控
 * 3. 内存使用分析
 * 4. 数据库性能追踪
 * 5. 热点方法识别
 */
public class PerformanceMonitor {
    
    private final EcoBridge plugin;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
        Thread.ofVirtual().factory()
    );
    
    // 指标收集
    private final LongAdder totalCalculations = new LongAdder();
    private final LongAdder simdCalculations = new LongAdder();
    private final LongAdder scalarCalculations = new LongAdder();
    private final LongAdder dbWrites = new LongAdder();
    private final LongAdder dbReads = new LongAdder();
    
    // 性能计数器
    private final ConcurrentHashMap<String, PerformanceCounter> counters = new ConcurrentHashMap<>();
    
    // JVM 监控
    private final MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
    private final ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
    private final GarbageCollectorMXBean youngGC;
    private final GarbageCollectorMXBean oldGC;
    
    // 基准测试结果
    private volatile long simdBandwidth = 0;  // GB/s
    private volatile int vectorLanes = 0;
    
    public PerformanceMonitor(EcoBridge plugin) {
        this.plugin = plugin;
        
        // 获取 GC Bean
        var gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
        youngGC = gcBeans.size() > 0 ? gcBeans.get(0) : null;
        oldGC = gcBeans.size() > 1 ? gcBeans.get(1) : null;
        
        // 启动基准测试
        benchmarkSIMD();
        
        // 启动定期监控
        startMonitoring();
    }
    
    /**
     * SIMD 基准测试
     */
    private void benchmarkSIMD() {
        Thread.ofVirtual().start(() -> {
            try {
                var species = jdk.incubator.vector.DoubleVector.SPECIES_PREFERRED;
                vectorLanes = species.length();
                
                int size = 1024 * 1024;  // 1M doubles = 8MB
                double[] data = new double[size];
                
                // 预热
                for (int i = 0; i < 10; i++) {
                    vectorAdd(data, species);
                }
                
                // 测试
                long startNs = System.nanoTime();
                int iterations = 100;
                for (int i = 0; i < iterations; i++) {
                    vectorAdd(data, species);
                }
                long elapsedNs = System.nanoTime() - startNs;
                
                // 计算带宽 (GB/s)
                long bytesProcessed = (long) size * 8 * iterations;
                double seconds = elapsedNs / 1e9;
                simdBandwidth = (long) (bytesProcessed / seconds / 1e9);
                
                plugin.getLogger().info(String.format(
                    "[Perf] SIMD Benchmark: %s (%d lanes), Bandwidth: %d GB/s",
                    species, vectorLanes, simdBandwidth
                ));
                
            } catch (Exception e) {
                plugin.getLogger().warning("[Perf] SIMD benchmark failed: " + e.getMessage());
            }
        });
    }
    
    private void vectorAdd(double[] data, VectorSpecies<Double> species) {
        int i = 0;
        int bound = species.loopBound(data.length);
        
        for (; i < bound; i += species.length()) {
            var v1 = jdk.incubator.vector.DoubleVector.fromArray(species, data, i);
            var v2 = v1.add(1.0);
            v2.intoArray(data, i);
        }
    }
    
    /**
     * 启动定期监控
     */
    private void startMonitoring() {
        int interval = plugin.getConfig().getInt("monitoring.log-interval", 300);
        
        scheduler.scheduleAtFixedRate(() -> {
            try {
                logPerformanceReport();
            } catch (Exception e) {
                plugin.getLogger().warning("[Perf] Monitoring error: " + e.getMessage());
            }
        }, interval, interval, TimeUnit.SECONDS);
    }
    
    /**
     * 记录性能报告
     */
    private void logPerformanceReport() {
        StringBuilder report = new StringBuilder();
        report.append("\n");
        report.append("╔════════════════════════════════════════════════════════════╗\n");
        report.append("║          EcoBridge Performance Report                     ║\n");
        report.append("╠════════════════════════════════════════════════════════════╣\n");
        
        // SIMD 统计
        long totalCalcs = totalCalculations.sum();
        long simdCalcs = simdCalculations.sum();
        double simdRatio = totalCalcs > 0 ? (100.0 * simdCalcs / totalCalcs) : 0.0;
        
        report.append(String.format("║ SIMD Vector: %s (%d lanes, %d GB/s)          \n", 
            getVectorSpeciesName(), vectorLanes, simdBandwidth));
        report.append(String.format("║ Calculations: %,d total, %,d SIMD (%.1f%%)    \n",
            totalCalcs, simdCalcs, simdRatio));
        
        // 数据库统计
        report.append(String.format("║ Database: %,d writes, %,d reads              \n",
            dbWrites.sum(), dbReads.sum()));
        
        // 内存统计
        MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
        long usedMB = heapUsage.getUsed() / 1048576;
        long maxMB = heapUsage.getMax() / 1048576;
        double memPercent = 100.0 * heapUsage.getUsed() / heapUsage.getMax();
        
        report.append(String.format("║ Memory: %,d MB / %,d MB (%.1f%%)             \n",
            usedMB, maxMB, memPercent));
        
        // GC 统计
        if (youngGC != null && oldGC != null) {
            report.append(String.format("║ GC: Young=%,d (%.1fs), Old=%,d (%.1fs)      \n",
                youngGC.getCollectionCount(), youngGC.getCollectionTime() / 1000.0,
                oldGC.getCollectionCount(), oldGC.getCollectionTime() / 1000.0));
        }
        
        // 线程统计
        int threadCount = threadBean.getThreadCount();
        int peakThreads = threadBean.getPeakThreadCount();
        report.append(String.format("║ Threads: %d active, %d peak                  \n",
            threadCount, peakThreads));
        
        // TPS 统计
        try {
            double[] tps = Bukkit.getTPS();
            report.append(String.format("║ TPS: %.2f (1m), %.2f (5m), %.2f (15m)       \n",
                tps[0], tps[1], tps[2]));
        } catch (Exception ignored) {}
        
        // 缓存统计
        if (plugin.getPidController() != null) {
            int cacheSize = plugin.getPidController().getCacheSize();
            int dirtySize = plugin.getPidController().getDirtyQueueSize();
            report.append(String.format("║ PID Cache: %,d items, %,d dirty             \n",
                cacheSize, dirtySize));
        }
        
        // 性能计数器
        if (!counters.isEmpty()) {
            report.append("╠════════════════════════════════════════════════════════════╣\n");
            report.append("║ Performance Counters:                                      ║\n");
            
            counters.forEach((name, counter) -> {
                double avgMs = counter.getAverageMs();
                long count = counter.getCount();
                report.append(String.format("║  %-30s: %.2fms (×%,d)\n", 
                    name, avgMs, count));
            });
        }
        
        report.append("╚════════════════════════════════════════════════════════════╝");
        
        plugin.getLogger().info(report.toString());
    }
    
    private String getVectorSpeciesName() {
        try {
            var species = jdk.incubator.vector.DoubleVector.SPECIES_PREFERRED;
            return species.toString().replace("DoubleVector", "");
        } catch (Exception e) {
            return "Unknown";
        }
    }
    
    /**
     * 性能计数器
     */
    public static class PerformanceCounter {
        private final AtomicLong totalNs = new AtomicLong(0);
        private final AtomicLong count = new AtomicLong(0);
        
        public void record(long nanoTime) {
            totalNs.addAndGet(nanoTime);
            count.incrementAndGet();
        }
        
        public double getAverageMs() {
            long c = count.get();
            return c > 0 ? (totalNs.get() / c / 1_000_000.0) : 0.0;
        }
        
        public long getCount() {
            return count.get();
        }
    }
    
    // ==================== 公共接口 ====================
    
    public void recordCalculation(boolean usedSIMD) {
        totalCalculations.increment();
        if (usedSIMD) {
            simdCalculations.increment();
        } else {
            scalarCalculations.increment();
        }
    }
    
    public void recordDbWrite() {
        dbWrites.increment();
    }
    
    public void recordDbRead() {
        dbReads.increment();
    }
    
    public void startTimer(String name) {
        counters.computeIfAbsent(name, k -> new PerformanceCounter());
    }
    
    public void stopTimer(String name, long startNs) {
        PerformanceCounter counter = counters.get(name);
        if (counter != null) {
            counter.record(System.nanoTime() - startNs);
        }
    }
    
    /**
     * 生成诊断报告
     */
    public String generateDiagnostics() {
        StringBuilder sb = new StringBuilder();
        
        sb.append("=== EcoBridge Diagnostics ===\n");
        sb.append("\n[System Info]\n");
        sb.append("Java: ").append(System.getProperty("java.version")).append("\n");
        sb.append("OS: ").append(System.getProperty("os.name")).append(" ")
          .append(System.getProperty("os.version")).append("\n");
        sb.append("Processors: ").append(Runtime.getRuntime().availableProcessors()).append("\n");
        
        sb.append("\n[SIMD Support]\n");
        sb.append("Vector Species: ").append(getVectorSpeciesName()).append("\n");
        sb.append("Lanes: ").append(vectorLanes).append("\n");
        sb.append("Bandwidth: ").append(simdBandwidth).append(" GB/s\n");
        
        sb.append("\n[Memory]\n");
        MemoryUsage heap = memoryBean.getHeapMemoryUsage();
        sb.append("Heap Used: ").append(heap.getUsed() / 1048576).append(" MB\n");
        sb.append("Heap Max: ").append(heap.getMax() / 1048576).append(" MB\n");
        
        MemoryUsage nonHeap = memoryBean.getNonHeapMemoryUsage();
        sb.append("Non-Heap Used: ").append(nonHeap.getUsed() / 1048576).append(" MB\n");
        
        sb.append("\n[Performance]\n");
        long totalCalcs = totalCalculations.sum();
        long simdCalcs = simdCalculations.sum();
        sb.append("Total Calculations: ").append(totalCalcs).append("\n");
        sb.append("SIMD Calculations: ").append(simdCalcs).append("\n");
        sb.append("SIMD Ratio: ").append(String.format("%.2f%%", 
            totalCalcs > 0 ? 100.0 * simdCalcs / totalCalcs : 0)).append("\n");
        
        return sb.toString();
    }
    
    public void shutdown() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
        }
    }
}