package top.ellan.ecobridge;

import org.bukkit.Bukkit;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitRunnable;

public class EcoBridge extends JavaPlugin {

    private static EcoBridge instance;
    private DatabaseManager databaseManager;
    private PidController pidController;
    private MarketManager marketManager;
    private IntegrationManager integrationManager;

    @Override
    public void onEnable() {
        instance = this;
        saveDefaultConfig();

        // 1. 初始化核心子系统
        // PidController 会分配 FFM 堆外内存 Arena
        this.databaseManager = new DatabaseManager(this);
        this.pidController = new PidController(this);
        // MarketManager 会启动虚拟线程 Executor
        this.marketManager = new MarketManager(this);
        // IntegrationManager 初始化
        this.integrationManager = new IntegrationManager(this);

        // 2. 异步初始化数据库连接池 (防止卡顿主线程)
        databaseManager.initPool();

        // 3. 注册 PlaceholderAPI (变量支持)
        if (Bukkit.getPluginManager().getPlugin("PlaceholderAPI") != null) {
            new EcoBridgeExpansion(this).register();
            getLogger().info("PlaceholderAPI hook registered.");
        } else {
            getLogger().warning("PlaceholderAPI not found! Variables will not work.");
        }

        // [新增] 4. 注册指令 (核心功能补全)
        if (getCommand("ecobridge") != null) {
            getCommand("ecobridge").setExecutor(new EcoBridgeCommand(this));
            getLogger().info("Commands registered.");
        } else {
            getLogger().severe("Failed to register command 'ecobridge'. Is it defined in plugin.yml?");
        }

        // 5. 启动定时任务
        startSchedulers();

        getLogger().info("EcoBridge (Java 25 SIMD + FFM + VirtualThreads) loaded successfully.");
    }

    @Override
    public void onDisable() {
        getLogger().info("Disabling EcoBridge...");

        // 1. 处理 PID 控制器资源
        if (pidController != null) {
            getLogger().info("Flushing PID buffer...");
            pidController.flushBuffer(true); // 同步刷写剩余数据到数据库
            
            // [关键] 关闭 FFM Arena 以释放堆外内存，防止内存泄漏
            pidController.close(); 
        }

        // 2. 处理市场管理器资源
        if (marketManager != null) {
            // [关键] 关闭虚拟线程池和 HttpClient
            marketManager.shutdown();
        }

        // 3. 关闭数据库连接池
        if (databaseManager != null) {
            databaseManager.closePool();
            getLogger().info("Database pool closed.");
        }
    }

    public static EcoBridge getInstance() { return instance; }
    public DatabaseManager getDatabaseManager() { return databaseManager; }
    public PidController getPidController() { return pidController; }
    public MarketManager getMarketManager() { return marketManager; }
    public IntegrationManager getIntegrationManager() { return integrationManager; }

    private void startSchedulers() {
        // [任务 1] 数据采集与计算 - 每 1 分钟 (1200 ticks)
        // 流程：主线程采集 -> 异步 SIMD 计算 -> 异步/同步 DB 写入
        new BukkitRunnable() {
            @Override
            public void run() {
                // 采集必须在主线程 (涉及 Bukkit/UltimateShop API)
                // 内部会自动提交 calculateBatch 到异步线程
                integrationManager.collectDataAndCalculate();
                
                // 刷写缓冲区 (内部会自动提交到虚拟线程)
                pidController.flushBuffer(false); 
                
                // 更新 TPS/玩家活跃度 (必须在主线程获取 TPS)
                marketManager.updateActivityFactor();
            }
        }.runTaskTimer(this, 1200L, 1200L);

        // [任务 2] 宏观经济指标更新 - 每 30 分钟
        // 计算全服财富门槛和通胀率 (完全异步，使用虚拟线程)
        new BukkitRunnable() {
            @Override
            public void run() {
                marketManager.updateEconomyMetrics();
            }
        }.runTaskTimerAsynchronously(this, 100L, 36000L);

        // [任务 3] 市场波动与节假日 - 每 5 分钟
        // 更新随机波动值和 HTTP 节假日缓存 (完全异步)
        new BukkitRunnable() {
            @Override
            public void run() {
                marketManager.updateMarketFlux();
                marketManager.updateHolidayCache();
            }
        }.runTaskTimerAsynchronously(this, 20L, 6000L);
    }
}