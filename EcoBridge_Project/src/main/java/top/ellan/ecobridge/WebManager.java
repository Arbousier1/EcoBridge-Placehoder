package top.ellan.ecobridge;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.atomic.*;

/**
 * Java 25 极限版 WebManager (Final Production Build - Patch v2)
 *
 * 修正日志:
 * 1. [CRITICAL] 移除 String.format 修复 Locale 小数点/逗号问题
 * 2. [CRITICAL] 移除错误日志过滤器，实现实时报错
 * 3. [FEATURE] 支持 node-api-url 热重载
 * 4. [TWEAK] 增加 HTTP 超时时间以适应云端冷启动
 */
public class WebManager {

    private final EcoBridge plugin;
    private volatile boolean running = true;

    // ==================== 配置 ====================
    // [Fix] 去掉 final，允许热重载更新地址
    private String nodeApiUrl;
    private static final int MAX_BATCH_SIZE = 64;
    private static final long FLUSH_INTERVAL_MS = 50;

    // ==================== 1. SoA 存储区域 (RingBuffer) ====================
    private final int capacity;
    private final int indexMask;

    private final long[] tsBuffer;
    private final double[] infBuffer;
    private final double[] tpsBuffer;
    private final int[] memBuffer;
    private final int[] threadBuffer;

    // ==================== 2. 游标控制 ====================
    private static final VarHandle WRITE_CURSOR;
    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            WRITE_CURSOR = l.findVarHandle(WebManager.class, "writeSequence", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    // Padding to prevent False Sharing
    @SuppressWarnings("unused")
    private long p1, p2, p3, p4, p5, p6, p7;
    
    @SuppressWarnings("unused")
    private volatile long writeSequence; 
    
    @SuppressWarnings("unused")
    private long p8, p9, p10, p11, p12, p13, p14, p15;

    // Read Cursor: 仅消费者修改
    private volatile long readSequence = 0L;

    // ==================== 3. 组件 ====================
    private final LockFreeAggregator aggregator;
    private final ExecutorService httpClientExecutor;
    private final HttpClient httpClient;
    private Thread senderThread;
    // [Fix] 移除 LongAdder errorCounter，不再隐藏错误

    public WebManager(EcoBridge plugin) {
        this.plugin = plugin;
        // [Fix] 初始化时调用配置加载
        reloadConfig();

        this.httpClientExecutor = Executors.newVirtualThreadPerTaskExecutor();

        this.httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .executor(httpClientExecutor)
                .connectTimeout(Duration.ofSeconds(5)) // [Fix] 增加超时防止冷启动断开
                .build();

        // 初始化 RingBuffer
        int configCap = plugin.getConfig().getInt("web.history-capacity", 1024);
        this.capacity = Integer.highestOneBit(configCap) == configCap ? configCap : Integer.highestOneBit(configCap) << 1;
        this.indexMask = capacity - 1;

        this.tsBuffer = new long[capacity];
        this.infBuffer = new double[capacity];
        this.tpsBuffer = new double[capacity];
        this.memBuffer = new int[capacity];
        this.threadBuffer = new int[capacity];

        this.aggregator = new LockFreeAggregator();

        startBatchSender();

        plugin.getLogger().info("[Web] Production Build Initialized | Target: " + nodeApiUrl);
    }

    // [Fix] 新增配置重载方法
    public void reloadConfig() {
        this.nodeApiUrl = plugin.getConfig().getString("web.node-api-url", "http://localhost:3000/api/update");
    }

    /**
     * 生产者：写入数据 (Lock-Free)
     */
    public void pushMetrics(double inflation, PerformanceMonitor.MonitorStats stats) {
        if (!running) return;

        long seq = (long) WRITE_CURSOR.getAndAddRelease(this, 1L);
        int idx = (int) (seq & indexMask);

        tsBuffer[idx] = System.currentTimeMillis();
        infBuffer[idx] = inflation;
        tpsBuffer[idx] = stats.tps();
        memBuffer[idx] = (int) stats.memoryUsedMB();
        threadBuffer[idx] = stats.threadCount();

        VarHandle.releaseFence();

        aggregator.observe(stats.tps(), stats.memoryUsedMB());
    }

    private void startBatchSender() {
        this.senderThread = Thread.ofVirtual()
                .name("web-batch-sender")
                .start(this::batchSendLoop);
    }

    private void batchSendLoop() {
        long lastFlushTime = System.currentTimeMillis();

        while (running) {
            long writeSeq = (long) WRITE_CURSOR.getAcquire(this);
            long available = writeSeq - readSequence;

            // 溢出保护
            if (available > capacity) {
                plugin.getLogger().warning("[Web] Buffer overflow! Dropping " + (available - capacity) + " metrics.");
                readSequence = writeSeq - capacity; 
                available = capacity;
            }

            if (available <= 0) {
                LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(200));
                continue;
            }

            int batchSize = (int) Math.min(available, MAX_BATCH_SIZE);
            
            long now = System.currentTimeMillis();
            if (batchSize < MAX_BATCH_SIZE && (now - lastFlushTime < FLUSH_INTERVAL_MS)) {
                 LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(200));
                 continue;
            }

            String payload = buildBatchJson(readSequence, batchSize);

            try {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(nodeApiUrl))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(payload))
                        .timeout(Duration.ofSeconds(5))
                        .build();

                HttpResponse<Void> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());

                if (response.statusCode() == 200) {
                    readSequence += batchSize;
                    lastFlushTime = System.currentTimeMillis();
                } else {
                    // [Fix] 详细记录非200状态码
                    handleError("HTTP " + response.statusCode());
                    readSequence += batchSize; 
                }

            } catch (Exception e) {
                // [Fix] 详细记录异常信息
                handleError(e.getClass().getSimpleName() + ": " + e.getMessage());
                readSequence += batchSize;
            }
        }
    }

    private String buildBatchJson(long startSeq, int size) {
        StringBuilder sb = new StringBuilder(32 + size * 80);
        sb.append("{\"batch\":[");

        for (int i = 0; i < size; i++) {
            int idx = (int) ((startSeq + i) & indexMask);
            sb.append('{')
              .append("\"ts\":").append(tsBuffer[idx]).append(',')
              // [Fix] 彻底修复 Locale 问题：直接 append double 值
              // 这样会使用标准的 "123.456" 格式，而不是 "123,456"
              .append("\"inf\":").append(infBuffer[idx]).append(',')
              .append("\"tps\":").append(tpsBuffer[idx]).append(',')
              .append("\"mem\":").append(memBuffer[idx]).append(',')
              .append("\"threads\":").append(threadBuffer[idx])
              .append('}');

            if (i < size - 1) sb.append(',');
        }
        sb.append("]}");
        return sb.toString();
    }

    private void handleError(String reason) {
        // [Fix] 移除 256 次过滤，直接报错，方便调试
        plugin.getLogger().warning("[Web Push Error] " + reason);
    }

    public void shutdown() {
        running = false;
        
        if (senderThread != null) {
            LockSupport.unpark(senderThread);
            senderThread.interrupt();
        }

        if (httpClientExecutor != null) {
            httpClientExecutor.shutdownNow();
        }

        plugin.getLogger().info("[Web] Production Build Shutdown Gracefully");
    }

    // ==================== 内部聚合器 ====================
    public static class LockFreeAggregator {
        private final DoubleAccumulator minTps = new DoubleAccumulator(Math::min, 20.0);
        private final DoubleAccumulator maxTps = new DoubleAccumulator(Math::max, 0.0);
        private final LongAccumulator maxMemory = new LongAccumulator(Math::max, 0L);

        private static final VarHandle LAST_RESET;
        static {
            try {
                MethodHandles.Lookup l = MethodHandles.lookup();
                LAST_RESET = l.findVarHandle(LockFreeAggregator.class, "lastResetTime", long.class);
            } catch (ReflectiveOperationException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        @SuppressWarnings("unused")
        private volatile long lastResetTime = System.currentTimeMillis();
        private final long windowSizeMs;

        public LockFreeAggregator() {
            this.windowSizeMs = TimeUnit.MINUTES.toMillis(5);
        }

        public void observe(double tps, long memory) {
            long now = System.currentTimeMillis();
            long lastReset = (long) LAST_RESET.getOpaque(this);

            if (now - lastReset > windowSizeMs) {
                if (LAST_RESET.compareAndSet(this, lastReset, now)) {
                    resetStats();
                }
            }

            minTps.accumulate(tps);
            maxTps.accumulate(tps);
            maxMemory.accumulate(memory);
        }

        public double getMinTps() { return minTps.get(); }
        public double getMaxTps() { return maxTps.get(); }
        public long getMaxMemory() { return maxMemory.get(); }

        private void resetStats() {
            minTps.reset();
            maxTps.reset();
            maxMemory.reset();
        }
        
        public void reset() {
            resetStats();
        }
    }
}