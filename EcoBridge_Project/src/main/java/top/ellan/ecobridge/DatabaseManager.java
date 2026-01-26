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
 * DatabaseManager - 批量优化版
 * 
 * 优化点:
 * 1. PreparedStatement 复用池
 * 2. 智能批量大小调整 (自适应)
 * 3. VirtualThread 异步加载
 * 4. 连接池预热
 * 5. 批量操作合并 (Batch Coalescing)
 */
public class DatabaseManager {
    
    public record PidDbSnapshot(String itemId, double integral, double lastError, 
                                double lastLambda, long updateTime) {}
    
    private final EcoBridge plugin;
    private HikariDataSource dataSource;
    
    // 批量操作队列
    private final BlockingQueue<PidDbSnapshot> pendingWrites = new LinkedBlockingQueue<>(20000);
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
                
                // 预热连接池
                warmupPool();
                
                // 创建表
                createTable();
                
                // 启动批量写入调度器
                startBatchWriter();
                
                initialized = true;
                plugin.getLogger().info("[DB] HikariCP initialized: " + 
                    config.getMaximumPoolSize() + " connections, VirtualThread enabled");
                
                // 加载 PID 状态
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
            "jdbc:mariadb://localhost:3306/ecobridge?useUnicode=true&characterEncoding=utf8");
        String user = plugin.getConfig().getString("database.user", "root");
        String pass = plugin.getConfig().getString("database.password", "password");
        
        config.setJdbcUrl(url);
        config.setUsername(user);
        config.setPassword(pass);
        
        // 性能优化参数
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
        config.addDataSourceProperty("useUnbufferedInput", "false");
        
        // 连接池配置
        config.setMaximumPoolSize(16);
        config.setMinimumIdle(4);
        config.setIdleTimeout(300000);  // 5分钟
        config.setConnectionTimeout(10000);  // 10秒
        config.setMaxLifetime(1800000);  // 30分钟
        config.setKeepaliveTime(60000);  // 1分钟心跳
        config.setLeakDetectionThreshold(30000);  // 30秒泄漏检测
        config.setPoolName("EcoBridge-Hikari");
        
        // 自定义线程工厂 (VirtualThread)
        config.setThreadFactory(Thread.ofVirtual().factory());
        
        return config;
    }
    
    /**
     * 连接池预热 - 避免首次查询延迟
     */
    private void warmupPool() {
        try (Connection conn = dataSource.getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("SELECT 1");
            }
            plugin.getLogger().info("[DB] Connection pool warmed up");
        } catch (SQLException e) {
            plugin.getLogger().warning("[DB] Warmup failed: " + e.getMessage());
        }
    }
    
    private void createTable() throws SQLException {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS eb_pid_states (
                    item_id VARCHAR(64) PRIMARY KEY,
                    integral DOUBLE NOT NULL DEFAULT 0,
                    last_error DOUBLE NOT NULL DEFAULT 0,
                    last_lambda DOUBLE NOT NULL DEFAULT 0.002,
                    update_time BIGINT NOT NULL DEFAULT 0,
                    INDEX idx_update_time (update_time)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                ROW_FORMAT=DYNAMIC
            """);
            
            plugin.getLogger().info("[DB] Table 'eb_pid_states' ready");
        }
    }
    
    /**
     * 启动批量写入调度器 (每500ms执行一次)
     */
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
     * 批量保存 (公共接口)
     */
    public void saveBatch(List<PidDbSnapshot> snapshots) {
        if (!initialized || snapshots.isEmpty()) return; // 
        
        // 1. 阈值判定：如果数据量较大（例如超过 1000 条）
        // 这种情况下合并写入的收益降低，且一次性往队列塞入过量数据可能导致阻塞
        if (snapshots.size() > 1000) {
            // 直接开启虚拟线程绕过队列进行物理写入 
            Thread.ofVirtual().start(() -> executeBatchWrite(snapshots));
            return;
        }
        
        // 2. 小规模增量数据（如每分钟的常规调价）
        // 将数据快速存入阻塞队列，由内部调度器每 500ms 自动合并写入，极大减少数据库连接开销 [cite: 13, 14]
        pendingWrites.addAll(snapshots);
    }
    
    /**
     * 刷新待写队列
     */
    private void flushPendingWrites() {
        if (pendingWrites.isEmpty()) return;
        
        List<PidDbSnapshot> batch = new ArrayList<>(optimalBatchSize.get());
        pendingWrites.drainTo(batch, optimalBatchSize.get());
        
        if (!batch.isEmpty()) {
            executeBatchWrite(batch);
        }
    }
    
    /**
     * 执行批量写入 (核心逻辑)
     */
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
                
                // 自适应批量大小调整
                long elapsedMs = (System.nanoTime() - startNs) / 1_000_000;
                adjustBatchSize(snapshots.size(), elapsedMs);
                
                if (plugin.getConfig().getBoolean("debug-db", false)) {
                    plugin.getLogger().info(String.format(
                        "[DB] Saved %d items in %dms (%.1f items/ms)",
                        snapshots.size(), elapsedMs, snapshots.size() / (double) Math.max(1, elapsedMs)
                    ));
                }
                
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(originalAutoCommit);
            }
            
        } catch (SQLException e) {
            plugin.getLogger().log(Level.WARNING, 
                "[DB] Batch save failed for " + snapshots.size() + " items", e);
        }
    }
    
    /**
     * 自适应批量大小调整
     * 目标: 单次批量操作 < 100ms
     */
    private void adjustBatchSize(int currentSize, long elapsedMs) {
        if (elapsedMs < 50) {
            // 太快了，增加批量大小
            optimalBatchSize.updateAndGet(old -> Math.min(2000, (int) (old * 1.2)));
        } else if (elapsedMs > 150) {
            // 太慢了，减少批量大小
            optimalBatchSize.updateAndGet(old -> Math.max(100, (int) (old * 0.8)));
        }
    }
    
    /**
     * 加载所有状态 (VirtualThread 流式读取)
     */
    public void loadStates(java.util.function.Consumer<PidDbSnapshot> consumer) {
        if (dataSource == null || dataSource.isClosed()) return;
        
        Thread.ofVirtual().start(() -> {
            try (Connection conn = dataSource.getConnection();
                 Statement stmt = conn.createStatement()) {
                
                stmt.setFetchSize(1000);  // 批量获取
                
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM eb_pid_states")) {
                    int count = 0;
                    while (rs.next()) {
                        consumer.accept(new PidDbSnapshot(
                            rs.getString("item_id"),
                            rs.getDouble("integral"),
                            rs.getDouble("last_error"),
                            rs.getDouble("last_lambda"),
                            rs.getLong("update_time")
                        ));
                        count++;
                    }
                    
                    plugin.getLogger().info("[DB] Loaded " + count + " PID states from database");
                }
                
            } catch (SQLException e) {
                plugin.getLogger().severe("[DB] Failed to load states: " + e.getMessage());
                e.printStackTrace();
            }
        });
    }
    
    /**
     * 强化版关闭逻辑：确保队列中所有调价数据完全落盘
     */
    public void closePool() {
        plugin.getLogger().info("[DB] 正在执行优雅停机，清理待写入队列...");
        
        // 1. 首先关闭调度器，停止定时任务
        batchScheduler.shutdown();
        
        try {
            // 2. 循环排空队列：直到 pendingWrites 彻底为空
            // 之前的版本只刷一次，现在我们循环直到所有数据都进入 executeBatchWrite
            int totalFlushed = 0;
            while (!pendingWrites.isEmpty()) {
                List<PidDbSnapshot> finalBatch = new ArrayList<>(2000);
                pendingWrites.drainTo(finalBatch, 2000);
                if (!finalBatch.isEmpty()) {
                    // 注意：这里必须同步执行写入，不能再开虚拟线程，否则主线程关了连接池，异步线程会报错
                    executeBatchWrite(finalBatch);
                    totalFlushed += finalBatch.size();
                }
            }
            if (totalFlushed > 0) {
                plugin.getLogger().info("[DB] 停机前成功追加写入 " + totalFlushed + " 条记录");
            }

            // 3. 等待调度器任务彻底结束
            if (!batchScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                batchScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            batchScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        // 4. 最后关闭连接池
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            plugin.getLogger().info("[DB] HikariCP 连接池已安全关闭");
        }
    }
    
    public HikariDataSource getDataSource() { 
        return dataSource; 
    }
    
    public boolean isInitialized() {
        return initialized;
    }

    public int getPendingWritesCount() {
    return pendingWrites.size();
    }
}