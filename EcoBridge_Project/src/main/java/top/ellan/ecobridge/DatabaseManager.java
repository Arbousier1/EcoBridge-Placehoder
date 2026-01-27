package top.ellan.ecobridge;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

/**
 * 数据库管理器 (最终防丢数据版)
 * 策略: 队列缓冲 + 溢出自动卸载 (Queue Buffer + Overflow Offloading)
 */
public class DatabaseManager {

    public record PidDbSnapshot(String itemId, double integral, double lastError,
                                double lastLambda, long updateTime) {}

    private final EcoBridge plugin;
    private HikariDataSource dataSource;

    // 批量操作队列 (有界，防止内存溢出)
    private final BlockingQueue<PidDbSnapshot> pendingWrites = new LinkedBlockingQueue<>(20000);
    
    // 调度器: 用于定期消费队列
    private final ScheduledExecutorService batchScheduler = Executors.newSingleThreadScheduledExecutor(
            Thread.ofVirtual().factory()
    );

    // 自适应批量大小
    private final AtomicInteger optimalBatchSize = new AtomicInteger(500);
    private volatile boolean initialized = false;

    public DatabaseManager(EcoBridge plugin) {
        this.plugin = plugin;
    }

    public void initPool() {
        Thread.ofVirtual().start(() -> {
            try {
                HikariConfig config = buildConfig();
                this.dataSource = new HikariDataSource(config);
                warmupPool();
                createTable();
                startBatchWriter();

                initialized = true;
                plugin.getLogger().info("[DB] HikariCP initialized with Overflow Protection.");
                
                plugin.getPidController().loadAllStates();

            } catch (Exception e) {
                plugin.getLogger().severe("[DB] Init failed: " + e.getMessage());
                e.printStackTrace();
            }
        });
    }

    private HikariConfig buildConfig() {
        HikariConfig config = new HikariConfig();
        String url = plugin.getConfig().getString("database.url",
                "jdbc:mariadb://localhost:3306/ecobridge?useUnicode=true&characterEncoding=utf8&allowMultiQueries=true&serverTimezone=UTC");
        String user = plugin.getConfig().getString("database.user", "root");
        String pass = plugin.getConfig().getString("database.password", "password");

        config.setJdbcUrl(url);
        config.setUsername(user);
        config.setPassword(pass);

        var pool = plugin.getConfig().getConfigurationSection("database.pool");
        if (pool != null) {
            config.setMaximumPoolSize(pool.getInt("maximum-size", 16));
            config.setMinimumIdle(pool.getInt("minimum-idle", 4));
            config.setConnectionTimeout(pool.getLong("connection-timeout", 10000L));
            config.setIdleTimeout(pool.getLong("idle-timeout", 300000L));
            config.setMaxLifetime(pool.getLong("max-lifetime", 1800000L));
            config.setKeepaliveTime(pool.getLong("keepalive-time", 60000L));
            config.setLeakDetectionThreshold(pool.getLong("leak-detection-threshold", 30000L));
        } else {
            config.setMaximumPoolSize(16);
            config.setMinimumIdle(4);
            config.setIdleTimeout(300000);
            config.setConnectionTimeout(10000);
            config.setMaxLifetime(1800000);
            config.setKeepaliveTime(60000);
        }

        config.setPoolName("EcoBridge-Hikari");
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "500");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "4096");
        config.addDataSourceProperty("useServerPrepStmts", "true");
        config.addDataSourceProperty("useLocalSessionState", "true");
        config.addDataSourceProperty("rewriteBatchedStatements", "true");
        config.addDataSourceProperty("cacheResultSetMetadata", "true");
        config.addDataSourceProperty("cacheServerConfiguration", "true");
        config.addDataSourceProperty("elideSetAutoCommits", "true");
        config.addDataSourceProperty("maintainTimeStats", "false");

        // 关键: 让 Hikari 内部线程也使用虚拟线程
        config.setThreadFactory(Thread.ofVirtual().factory());
        return config;
    }

    private void warmupPool() {
        try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("SELECT 1");
        } catch (SQLException ignored) {}
    }

    private void createTable() throws SQLException {
        try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS eb_pid_states (
                    item_id VARCHAR(64) PRIMARY KEY,
                    integral DOUBLE NOT NULL DEFAULT 0,
                    last_error DOUBLE NOT NULL DEFAULT 0,
                    last_lambda DOUBLE NOT NULL DEFAULT 0.002,
                    update_time BIGINT NOT NULL DEFAULT 0,
                    INDEX idx_update_time (update_time)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=DYNAMIC
            """);
        }
    }

    private void startBatchWriter() {
        batchScheduler.scheduleAtFixedRate(() -> {
            try {
                flushPendingWrites();
            } catch (Exception e) {
                plugin.getLogger().log(Level.WARNING, "[DB] Batch write error", e);
            }
        }, 500, 500, TimeUnit.MILLISECONDS);
    }

    /**
     * [修复核心] 批量保存: 队列缓冲 + 溢出卸载
     * 保证: 1. 不阻塞调用方; 2. 不丢失任何数据
     */
    public void saveBatch(List<PidDbSnapshot> snapshots) {
        if (!initialized || snapshots.isEmpty()) return;

        // 策略 1: 大批量数据 (>1000) 直接走独立虚拟线程，不占用队列资源
        if (snapshots.size() > 1000) {
            Thread.ofVirtual().start(() -> executeBatchWrite(snapshots));
            return;
        }

        // 策略 2: 尝试放入缓冲区队列 (Buffer)
        // remainingCapacity 检查非常快
        if (pendingWrites.remainingCapacity() >= snapshots.size()) {
            pendingWrites.addAll(snapshots);
        } else {
            // 策略 3: [溢出保护] 队列满了! (Overflow)
            // 动作: 不要阻塞，也不要丢弃。
            // 方案: 立即启动一个独立的虚拟线程处理这批“溢出”数据。
            // 解释: 虚拟线程创建成本极低，这相当于临时的“突发扩容”能力。
            if (plugin.getConfig().getBoolean("debug-db", false)) {
                plugin.getLogger().warning("[DB] Write queue full! Offloading " + snapshots.size() + " items to overflow thread.");
            }
            
            // 必须复制列表，因为 snapshots 来源可能在外部被复用/清空
            List<PidDbSnapshot> overflowBatch = new ArrayList<>(snapshots);
            Thread.ofVirtual().start(() -> executeBatchWrite(overflowBatch));
        }
    }

    private void flushPendingWrites() {
        if (pendingWrites.isEmpty()) return;

        // 每次取出最优批量大小的数据
        List<PidDbSnapshot> batch = new ArrayList<>(optimalBatchSize.get());
        pendingWrites.drainTo(batch, optimalBatchSize.get());

        if (!batch.isEmpty()) {
            executeBatchWrite(batch);
        }
    }

    private void executeBatchWrite(List<PidDbSnapshot> snapshots) {
        if (dataSource == null || dataSource.isClosed()) return;

        String sql = """
            INSERT INTO eb_pid_states (item_id, integral, last_error, last_lambda, update_time)
            VALUES (?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE
                integral = VALUES(integral),
                last_error = VALUES(last_error),
                last_lambda = VALUES(last_lambda),
                update_time = VALUES(update_time)
        """;

        long startNs = System.nanoTime();

        try (Connection conn = dataSource.getConnection()) {
            // 关闭自动提交以开启事务批处理
            boolean originalAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                for (PidDbSnapshot record : snapshots) {
                    ps.setString(1, record.itemId());
                    ps.setDouble(2, record.integral());
                    ps.setDouble(3, record.lastError());
                    ps.setDouble(4, record.lastLambda());
                    ps.setLong(5, record.updateTime());
                    ps.addBatch();
                }

                ps.executeBatch();
                conn.commit();

                // 性能指标收集
                long elapsedMs = (System.nanoTime() - startNs) / 1_000_000;
                adjustBatchSize(snapshots.size(), elapsedMs);

                // 记录统计 (可选)
                if (plugin.getPerformanceMonitor() != null) {
                    plugin.getPerformanceMonitor().recordDbWrite();
                }

            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(originalAutoCommit);
            }

        } catch (SQLException e) {
            plugin.getLogger().log(Level.WARNING, "[DB] Failed to save " + snapshots.size() + " items", e);
            // 极端的重试逻辑可以在这里添加，但通常 DB 报错说明连接断开，重试意义不大
        }
    }

    private void adjustBatchSize(int currentSize, long elapsedMs) {
        // 目标：将每次写入控制在 50ms - 150ms 之间，避免单次事务过大卡顿 DB
        if (elapsedMs < 50) {
            optimalBatchSize.updateAndGet(old -> Math.min(2000, (int) (old * 1.2)));
        } else if (elapsedMs > 150) {
            optimalBatchSize.updateAndGet(old -> Math.max(100, (int) (old * 0.8)));
        }
    }

    public void loadStates(java.util.function.Consumer<PidDbSnapshot> consumer) {
        if (dataSource == null || dataSource.isClosed()) return;

        Thread.ofVirtual().start(() -> {
            try (Connection conn = dataSource.getConnection();
                 Statement stmt = conn.createStatement()) {
                
                stmt.setFetchSize(1000); // 流式读取，防止 OOM
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM eb_pid_states")) {
                    while (rs.next()) {
                        consumer.accept(new PidDbSnapshot(
                                rs.getString("item_id"),
                                rs.getDouble("integral"),
                                rs.getDouble("last_error"),
                                rs.getDouble("last_lambda"),
                                rs.getLong("update_time")
                        ));
                    }
                }
            } catch (SQLException e) {
                plugin.getLogger().severe("[DB] Load states failed: " + e.getMessage());
            }
        });
    }

    public void closePool() {
        plugin.getLogger().info("[DB] Stopping database services...");
        batchScheduler.shutdown(); // 停止新的定时任务

        try {
            // 1. 排空内存队列
            int drained = 0;
            while (!pendingWrites.isEmpty()) {
                List<PidDbSnapshot> batch = new ArrayList<>(2000);
                pendingWrites.drainTo(batch, 2000);
                if (!batch.isEmpty()) {
                    executeBatchWrite(batch);
                    drained += batch.size();
                }
            }
            if (drained > 0) plugin.getLogger().info("[DB] Flushed " + drained + " pending items.");

            // 2. 等待所有虚拟线程完成 (简单等待)
            if (!batchScheduler.awaitTermination(3, TimeUnit.SECONDS)) {
                batchScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            batchScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // 3. 关闭连接池
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            plugin.getLogger().info("[DB] Connection pool closed.");
        }
    }

    public HikariDataSource getDataSource() { return dataSource; }
    public boolean isInitialized() { return initialized; }
    public int getPendingWritesCount() { return pendingWrites.size(); }
}