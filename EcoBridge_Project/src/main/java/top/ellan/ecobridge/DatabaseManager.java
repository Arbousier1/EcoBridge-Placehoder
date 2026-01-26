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

    // [新增] 数据传输对象 (DTO): 用于在主线程和数据库IO线程之间传递不可变数据
    // 使用 Record 减少样板代码，且不可变，线程安全
    public record PidDbSnapshot(String itemId, double integral, double lastError, double lastLambda, long updateTime) {}

    private final EcoBridge plugin;
    private HikariDataSource dataSource;

    public DatabaseManager(EcoBridge plugin) {
        this.plugin = plugin;
    }

    public void initPool() {
        // 在 onEnable 中异步执行，防止卡死主线程
        Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
            HikariConfig config = new HikariConfig();
            
            // 从配置文件读取 (如果未配置则使用默认值)
            String url = plugin.getConfig().getString("database.url", "jdbc:mariadb://localhost:3306/ecobridge");
            String user = plugin.getConfig().getString("database.user", "root");
            String pass = plugin.getConfig().getString("database.password", "password");
            
            config.setJdbcUrl(url);
            config.setUsername(user);
            config.setPassword(pass);

            // --- HikariCP 极致性能配置 (针对 MySQL/MariaDB) ---
            
            // 1. 开启预处理语句缓存
            config.addDataSourceProperty("cachePrepStmts", "true");
            config.addDataSourceProperty("prepStmtCacheSize", "250");
            config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
            
            // 2. 开启服务端预处理 (通常能提升性能)
            config.addDataSourceProperty("useServerPrepStmts", "true");
            config.addDataSourceProperty("useLocalSessionState", "true");
            
            // 3. [关键优化] 开启批量重写 
            // 将 insert into ... values (...), (...), (...) 合并为单条语句发送
            // 性能提升可达 10倍+
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
                
                // 初始化完成后，加载缓存
                plugin.getPidController().loadAllStates();
            } catch (Exception e) {
                plugin.getLogger().severe("Failed to init HikariCP: " + e.getMessage());
            }
        });
    }

    private void createTable() throws SQLException {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
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
     * * @param snapshots 不可变的快照列表，线程安全，来自 PidController 的缓冲区
     */
    public void saveBatch(List<PidDbSnapshot> snapshots) {
        if (snapshots.isEmpty() || dataSource == null || dataSource.isClosed()) return;

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
            // 关闭自动提交，手动控制事务以获得最大吞吐量
            boolean originalAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                // 遍历列表 (比遍历 Map.Entry 快)
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
            } catch (SQLException e) {
                conn.rollback();
                throw e; 
            } finally {
                conn.setAutoCommit(originalAutoCommit);
            }
        } catch (SQLException e) {
            plugin.getLogger().log(Level.WARNING, "Batch save failed for " + snapshots.size() + " items", e);
        }
    }
    
    /**
     * 加载数据
     * 使用 Consumer 回调，避免将整个 ResultSet 加载到内存 List 中
     */
    public void loadStates(java.util.function.Consumer<PidDbSnapshot> consumer) {
        if (dataSource == null || dataSource.isClosed()) return;
        
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             java.sql.ResultSet rs = stmt.executeQuery("SELECT * FROM eb_pid_states")) {
             
            // 流式处理
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