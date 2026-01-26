package top.ellan.ecobridge;

import jdk.incubator.vector.DoubleVector;
import jdk.incubator.vector.VectorSpecies;
import org.bukkit.Bukkit;
import org.bukkit.scheduler.BukkitTask;

import java.lang.management.*;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * PerformanceMonitor (Thread-Safe Edition)
 * 
 * 修复:
 * 1. 解决了 threadBean, youngGC, oldGC 未使用的警告 (现在已加入报告)
 * 2. 保持了 Paper/Spigot 的主线程安全原则
 * 3. 增强了 GC 和线程状态的监控输出
 */
public class PerformanceMonitor {
    
    private final EcoBridge plugin;
    private final ScheduledExecutorService scheduler;
    private BukkitTask syncCacheTask;
    
    // ==================== 线程安全的计数器 ====================
    private final LongAdder totalCalculations = new LongAdder();
    private final LongAdder simdCalculations = new LongAdder();
    private final LongAdder scalarCalculations = new LongAdder();
    private final LongAdder dbWrites = new LongAdder();
    private final LongAdder dbReads = new LongAdder();
    
    private final ConcurrentHashMap<String, PerformanceCounter> counters = new ConcurrentHashMap<>();
    
    // ==================== JVM 监控 Bean ====================
    private final MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
    private final ThreadMXBean threadBean = ManagementFactory.getThreadMXBean(); // [Fix] 现在被使用了
    private GarbageCollectorMXBean youngGC; // [Fix] 现在被使用了
    private GarbageCollectorMXBean oldGC;   // [Fix] 现在被使用了
    
    // ==================== 缓存状态 ====================
    private volatile double[] cachedTps = new double[]{20.0, 20.0, 20.0};
    private volatile int cachedPlayerCount = 0;
    private final int logInterval;
    
    private volatile long simdBandwidth = 0;
    private volatile int vectorLanes = 0;
    
    public PerformanceMonitor(EcoBridge plugin) {
        this.plugin = plugin;
        
        // 1. Config (Main Thread)
        this.logInterval = plugin.getConfig().getInt("monitoring.log-interval", 300);
        
        // 2. JVM Beans
        detectGCBeans();
        
        // 3. Scheduler
        this.scheduler = Executors.newSingleThreadScheduledExecutor(
            r -> Thread.ofVirtual().name("EcoBridge-Monitor").unstarted(r)
        );
        
        // 4. Sync Task for Bukkit API
        this.syncCacheTask = Bukkit.getScheduler().runTaskTimer(plugin, this::updateServerStateCache, 100L, 100L);
        
        // 5. Benchmark
        benchmarkSIMD();
        
        // 6. Start Monitor
        startMonitoring();
    }

    private void updateServerStateCache() {
        try {
            double[] tps = Bukkit.getTPS();
            if (tps != null) {
                this.cachedTps = tps;
            }
            this.cachedPlayerCount = Bukkit.getOnlinePlayers().size();
        } catch (Throwable ignored) {}
    }

    private void detectGCBeans() {
        List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
        for (GarbageCollectorMXBean bean : gcBeans) {
            String name = bean.getName().toLowerCase();
            if (name.contains("young") || name.contains("eden") || name.contains("copy") || name.contains("scavenge")) {
                youngGC = bean;
            } else if (name.contains("old") || name.contains("tenured") || name.contains("mark") || name.contains("global")) {
                oldGC = bean;
            } else if (name.contains("g1")) {
                if (name.contains("young")) youngGC = bean; else oldGC = bean;
            } else if (name.contains("shenandoah") || name.contains("zgc")) {
                oldGC = bean;
            }
        }
    }
    
    private void benchmarkSIMD() {
        Thread.ofVirtual().start(() -> {
            try {
                VectorSpecies<Double> species = DoubleVector.SPECIES_PREFERRED;
                vectorLanes = species.length();
                int size = 1024 * 1024;
                double[] data = new double[size];
                
                for (int i = 0; i < 50; i++) vectorAdd(data, species);
                
                long startNs = System.nanoTime();
                int iterations = 100;
                for (int i = 0; i < iterations; i++) vectorAdd(data, species);
                long elapsedNs = System.nanoTime() - startNs;
                
                long bytesProcessed = (long) size * 8L * iterations;
                double seconds = elapsedNs / 1e9;
                simdBandwidth = (long) (bytesProcessed / seconds / 1e9);
                
                plugin.getLogger().info(String.format("[Perf] SIMD Active: %s (%d lanes), BW: %d GB/s", 
                    species, vectorLanes, simdBandwidth));
            } catch (Throwable e) {
                plugin.getLogger().warning("[Perf] SIMD unavailable: " + e.getMessage());
            }
        });
    }
    
    private void vectorAdd(double[] data, VectorSpecies<Double> species) {
        int i = 0;
        int bound = species.loopBound(data.length);
        for (; i < bound; i += species.length()) {
            DoubleVector.fromArray(species, data, i).add(1.0).intoArray(data, i);
        }
    }
    
    private void startMonitoring() {
        scheduler.scheduleAtFixedRate(this::logPerformanceReport, logInterval, logInterval, TimeUnit.SECONDS);
    }
    
    private void logPerformanceReport() {
        try {
            StringBuilder report = new StringBuilder();
            report.append("\n§8[§aEcoBridge§8] §7Performance Diagnostics\n");
            
            // --- 1. 计算统计 ---
            long total = totalCalculations.sum();
            long simd = simdCalculations.sum();
            double ratio = total > 0 ? (100.0 * simd / total) : 0.0;
            report.append(String.format(" §7SIMD: §f%d lanes §7| Ratio: §a%.1f%%\n", vectorLanes, ratio));
            
            // --- 2. 内存 & 线程 (使用 threadBean) ---
            MemoryUsage heap = memoryBean.getHeapMemoryUsage();
            long used = heap.getUsed() / 1048576;
            long max = heap.getMax() / 1048576;
            // [Fix] 使用 threadBean 统计线程数
            report.append(String.format(" §7Mem: §e%dMB/%dMB §7| Threads: §f%d\n", 
                used, max, threadBean.getThreadCount()));
            
            // --- 3. GC 统计 (使用 youngGC/oldGC) ---
            if (youngGC != null) {
                report.append(String.format(" §7GC(Young): §f%d runs §7| §e%dms\n", 
                    youngGC.getCollectionCount(), youngGC.getCollectionTime()));
            }
            if (oldGC != null) {
                report.append(String.format(" §7GC(Old):   §f%d runs §7| §c%dms\n", 
                    oldGC.getCollectionCount(), oldGC.getCollectionTime()));
            }

            // --- 4. Server 状态 ---
            report.append(String.format(" §7TPS: §a%.1f §7| Players: §f%d\n", cachedTps[0], cachedPlayerCount));
            
            // --- 5. 数据库 & 热点 ---
            report.append(String.format(" §7DB IO: §fW:%,d R:%,d\n", dbWrites.sumThenReset(), dbReads.sumThenReset()));
            
            if (!counters.isEmpty()) {
                report.append(" §7Hotspots:\n");
                counters.forEach((k, v) -> {
                    if (v.getCount() > 0) {
                        report.append(String.format("  - %s: §e%.3fms\n", k, v.getAverageMs()));
                    }
                });
            }
            
            plugin.getLogger().info(report.toString());
            
            // Reset counters
            totalCalculations.reset();
            simdCalculations.reset();
            scalarCalculations.reset();
            
        } catch (Exception e) {
            plugin.getLogger().warning("Error generating perf report: " + e.getMessage());
        }
    }
    
    // ==================== 辅助类 ====================
    
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
        
        public long getCount() { return count.get(); }
    }
    
    // ==================== Public API ====================
    
    public void recordCalculation(boolean usedSIMD) {
        totalCalculations.increment();
        if (usedSIMD) simdCalculations.increment(); else scalarCalculations.increment();
    }
    
    public void recordDbWrite() { dbWrites.increment(); }
    public void recordDbRead() { dbReads.increment(); }
    
    public void startTimer(String name) {
        counters.computeIfAbsent(name, k -> new PerformanceCounter());
    }
    
    public void stopTimer(String name, long startNs) {
        PerformanceCounter c = counters.get(name);
        if (c != null) c.record(System.nanoTime() - startNs);
    }
    
    public void shutdown() {
        if (syncCacheTask != null && !syncCacheTask.isCancelled()) {
            syncCacheTask.cancel();
        }
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(2, TimeUnit.SECONDS)) scheduler.shutdownNow();
        } catch (InterruptedException e) { scheduler.shutdownNow(); }
    }
}