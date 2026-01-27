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
import java.util.concurrent.atomic.DoubleAccumulator;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

/**
 * Java 25 极限版 WebManager (Final Production Build)
 *
 * 修正点：
 * 1. 修复 readSequence 的多线程竞态条件 (移至 Consumer 侧控制)
 * 2. RingBuffer 显式溢出保护
 * 3. HttpClient 强制绑定虚拟线程 Executor
 * 4. Microsecond 级别休眠
 * 5. 优雅停机
 * 6. [Fix] 移除未使用的 plugin 字段引用
 */
public class WebManager {

    private final EcoBridge plugin;
    private volatile boolean running = true;

    // ==================== 配置 ====================
    private final String nodeApiUrl;
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
    private final LongAdder errorCounter = new LongAdder();

    public WebManager(EcoBridge plugin) {
        this.plugin = plugin;
        this.nodeApiUrl = plugin.getConfig().getString("web.node-api-url", "http://localhost:3000/api/update");

        this.httpClientExecutor = Executors.newVirtualThreadPerTaskExecutor();

        this.httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .executor(httpClientExecutor)
                .connectTimeout(Duration.ofSeconds(5))
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

        // [Fix] 不再传递 plugin 参数，因为 LockFreeAggregator 内部不使用它
        this.aggregator = new LockFreeAggregator();

        startBatchSender();

        plugin.getLogger().info("[Web] Production Build Initialized | Buffer: " + capacity + " | VThread Executor Active");
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
                        .timeout(Duration.ofSeconds(2))
                        .build();

                HttpResponse<Void> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());

                if (response.statusCode() == 200) {
                    readSequence += batchSize;
                    lastFlushTime = System.currentTimeMillis();
                } else {
                    handleError("Status " + response.statusCode());
                    readSequence += batchSize; 
                }

            } catch (Exception e) {
                handleError(e.getMessage());
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
        errorCounter.increment();
        long count = errorCounter.sum();
        if ((count & 0xFF) == 0) {
            plugin.getLogger().warning("[Web] Push failed x256. Reason: " + reason);
        }
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

    // ==================== 内部聚合器 (Fixed) ====================
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
        // [Fix] Removed unused 'plugin' field

        public LockFreeAggregator() { // [Fix] Removed 'plugin' parameter
            // [Fix] Removed 'this.plugin = plugin;'
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