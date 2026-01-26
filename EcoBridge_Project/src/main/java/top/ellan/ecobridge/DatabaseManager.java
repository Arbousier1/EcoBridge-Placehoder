package top.ellan.ecobridge;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.bukkit.Bukkit;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.logging.Level;

public class DatabaseManager {

    // [DTO] 数据快照，使用 Record 保证不可变性和线程安全
    public record PidDbSnapshot(String itemId, double integral, double lastError, double lastLambda, long updateTime) {}

    private final EcoBridge plugin;
    private HikariDataSource dataSource;

    public DatabaseManager(EcoBridge plugin) {
        this.plugin = plugin;
    }

    public void initPool() {
        // 异步初始化连接池，避免阻塞主线程
        Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
            HikariConfig config = new HikariConfig();
            
            // 读取配置
            String url = plugin.getConfig().getString("database.url", "jdbc:mariadb://localhost:3306/ecobridge");
            String user = plugin.getConfig().getString("database.user", "root");
            String pass = plugin.getConfig().getString("database.password", "password");
            
            config.setJdbcUrl(url);
            config.setUsername(user);
            config.setPassword(pass);

            // --- HikariCP 针对 MySQL/MariaDB 的高性能配置 ---
            
            // 1. 预处理语句缓存
            config.addDataSourceProperty("cachePrepStmts", "true");
            config.addDataSourceProperty("prepStmtCacheSize", "250");
            config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
            
            // 2. 服务端预处理
            config.addDataSourceProperty("useServerPrepStmts", "true");
            config.addDataSourceProperty("useLocalSessionState", "true");
            
            // 3. [关键] 批量重写 (Rewrite Batched Statements)
            // 将多条 INSERT 语句合并为单条发送，性能提升巨大
            config.addDataSourceProperty("rewriteBatchedStatements", "true");
            
            config.addDataSourceProperty("cacheResultSetMetadata", "true");
            config.addDataSourceProperty("cacheServerConfiguration", "true");
            config.addDataSourceProperty("elideSetAutoCommits", "true");
            config.addDataSourceProperty("maintainTimeStats", "false");

            config.setMaximumPoolSize(10);
            config.setMinimumIdle(2);
            config.setIdleTimeout(30000);
            config.setConnectionTimeout(5000);
            config.setPoolName("EcoBridge-Hikari");

            try {
                this.dataSource = new HikariDataSource(config);
                createTable();
                plugin.getLogger().info("HikariCP Pool initialized successfully.");
                
                // 初始化完成后加载数据到内存
                plugin.getPidController().loadAllStates();
            } catch (Exception e) {
                plugin.getLogger().severe("Failed to init HikariCP: " + e.getMessage());
            }
        });
    }

    private void createTable() throws SQLException {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            // 创建表结构
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS eb_pid_states (
                    item_id VARCHAR(64) PRIMARY KEY, 
                    integral DOUBLE, 
                    last_error DOUBLE, 
                    last_lambda DOUBLE, 
                    update_time BIGINT
                )
            """);
        }
    }

    /**
     * 高性能批量保存
     * 自动使用事务和 rewriteBatchedStatements 优化写入
     * @param snapshots 数据快照列表
     */
    public void saveBatch(List<PidDbSnapshot> snapshots) {
        if (snapshots.isEmpty() || dataSource == null || dataSource.isClosed()) return;

        // ON DUPLICATE KEY UPDATE 实现 "存在即更新，不存在即插入"
        String sql = """
            INSERT INTO eb_pid_states (item_id, integral, last_error, last_lambda, update_time) 
            VALUES (?, ?, ?, ?, ?) 
            ON DUPLICATE KEY UPDATE 
                integral = VALUES(integral), 
                last_error = VALUES(last_error), 
                last_lambda = VALUES(last_lambda), 
                update_time = VALUES(update_time)
        """;

        try (Connection conn = dataSource.getConnection()) {
            // 手动开启事务
            boolean originalAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                for (PidDbSnapshot record : snapshots) {
                    ps.setString(1, record.itemId());
                    ps.setDouble(2, record.integral());
                    ps.setDouble(3, record.lastError());
                    ps.setDouble(4, record.lastLambda());
                    ps.setLong(5, record.updateTime());
                    ps.addBatch(); // 添加到批处理队列
                }
                
                ps.executeBatch(); // 执行批量操作
                conn.commit();     // 提交事务
                
            } catch (SQLException e) {
                conn.rollback();   // 出错回滚
                throw e; 
            } finally {
                conn.setAutoCommit(originalAutoCommit);
            }
        } catch (SQLException e) {
            plugin.getLogger().log(Level.WARNING, "Batch save failed for " + snapshots.size() + " items", e);
        }
    }
    
    /**
     * 流式加载数据
     * 使用 Consumer 回调处理每一行，避免一次性加载大量对象占用堆内存
     */
    public void loadStates(java.util.function.Consumer<PidDbSnapshot> consumer) {
        if (dataSource == null || dataSource.isClosed()) return;
        
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             java.sql.ResultSet rs = stmt.executeQuery("SELECT * FROM eb_pid_states")) {
             
            // 设置 fetch size 暗示驱动流式读取，但这取决于驱动实现
            stmt.setFetchSize(1000); 

            while (rs.next()) {
                consumer.accept(new PidDbSnapshot(
                    rs.getString("item_id"),
                    rs.getDouble("integral"),
                    rs.getDouble("last_error"),
                    rs.getDouble("last_lambda"),
                    rs.getLong("update_time")
                ));
            }
        } catch (SQLException e) {
            plugin.getLogger().severe("Failed to load states: " + e.getMessage());
        }
    }

    public void closePool() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
        }
    }
    
    public HikariDataSource getDataSource() { return dataSource; }
}