package top.ellan.ecobridge;

import org.bukkit.Bukkit;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitRunnable;

/**
 * EcoBridge - 主插件类
 * 
 * 架构优化:
 * 1. 性能监控集成
 * 2. 优雅启动/关闭序列
 * 3. 健康检查机制
 * 4. 异常隔离处理
 */
public class EcoBridge extends JavaPlugin {

    private static EcoBridge instance;
    
    // 核心管理器
    private DatabaseManager databaseManager;
    private PidController pidController;
    private MarketManager marketManager;
    private IntegrationManager integrationManager;
    
    // [新增] 性能监控器
    private PerformanceMonitor performanceMonitor;
    
    // [新增] 初始化状态标记
    private volatile boolean fullyInitialized = false;

    @Override
    public void onEnable() {
        instance = this;
        
        long startTime = System.currentTimeMillis();
        getLogger().info("═══════════════════════════════════════════════");
        getLogger().info("  EcoBridge - Java 25 Extreme Performance");
        getLogger().info("  SIMD + FFM + VirtualThreads + SoA Layout");
        getLogger().info("═══════════════════════════════════════════════");
        
        try {
            // 0. 保存默认配置
            saveDefaultConfig();
            
            // 1. 初始化核心子系统 (按依赖顺序)
            initializeComponents();
            
            // 2. 异步初始化数据库 (防止主线程阻塞)
            databaseManager.initPool();
            
            // 3. 注册 PlaceholderAPI 扩展
            registerPlaceholderAPI();
            
            // 4. 注册命令处理器
            registerCommands();
            
            // 5. 启动定时调度器
            startSchedulers();
            
            // 6. 启动性能监控 (如果启用)
            if (getConfig().getBoolean("monitoring.enabled", true)) {
                performanceMonitor = new PerformanceMonitor(this);
                getLogger().info("Performance monitoring enabled");
            }
            
            // 7. 预热关键路径 (减少首次调用延迟)
            warmupCriticalPaths();
            
            fullyInitialized = true;
            
            long elapsedMs = System.currentTimeMillis() - startTime;
            getLogger().info("═══════════════════════════════════════════════");
            getLogger().info("  ✓ EcoBridge loaded successfully in " + elapsedMs + "ms");
            getLogger().info("═══════════════════════════════════════════════");
            
        } catch (Exception e) {
            getLogger().severe("═══════════════════════════════════════════════");
            getLogger().severe("  ✗ FATAL: Failed to initialize EcoBridge");
            getLogger().severe("═══════════════════════════════════════════════");
            e.printStackTrace();
            
            // 清理已初始化的资源
            emergencyShutdown();
            Bukkit.getPluginManager().disablePlugin(this);
        }
    }

    @Override
    public void onDisable() {
        getLogger().info("═══════════════════════════════════════════════");
        getLogger().info("  Gracefully shutting down EcoBridge...");
        getLogger().info("═══════════════════════════════════════════════");
        
        fullyInitialized = false;
        
        try {
            // 优雅关闭序列 (反向依赖顺序)
            
            // 1. 停止性能监控
            if (performanceMonitor != null) {
                getLogger().info("[1/6] Stopping performance monitor...");
                performanceMonitor.shutdown();
            }
            
            // 2. 刷新 PID 控制器缓冲区
            if (pidController != null) {
                getLogger().info("[2/6] Flushing PID buffer...");
                int pendingWrites = pidController.getDirtyQueueSize();
                if (pendingWrites > 0) {
                    getLogger().info("  → " + pendingWrites + " pending writes");
                }
                
                // 同步刷写,确保数据不丢失
                pidController.flushBuffer(true);
                
                // 等待数据库完成写入 (最多 5 秒)
                waitForDatabaseFlush(5000);
                
                // 关闭 FFM Arena
                getLogger().info("  → Closing FFM Arena...");
                pidController.close();
            }
            
            // 3. 关闭市场管理器 (停止虚拟线程池和 HTTP 客户端)
            if (marketManager != null) {
                getLogger().info("[3/6] Shutting down market manager...");
                marketManager.shutdown();
            }
            
            // 4. 关闭集成管理器
            if (integrationManager != null) {
                getLogger().info("[4/6] Closing integration manager...");
                // IntegrationManager 无需特殊清理
            }
            
            // 5. 关闭数据库连接池 (最后关闭,确保所有写入完成)
            if (databaseManager != null) {
                getLogger().info("[5/6] Closing database pool...");
                databaseManager.closePool();
            }
            
            // 6. 清理静态引用
            getLogger().info("[6/6] Cleaning up references...");
            instance = null;
            
            getLogger().info("═══════════════════════════════════════════════");
            getLogger().info("  ✓ EcoBridge shutdown complete");
            getLogger().info("═══════════════════════════════════════════════");
            
        } catch (Exception e) {
            getLogger().severe("Error during shutdown: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // ==================== 初始化方法 ====================
    
    private void initializeComponents() {
        getLogger().info("Initializing core components...");
        
        // 顺序很重要: DatabaseManager -> PidController -> MarketManager -> IntegrationManager
        this.databaseManager = new DatabaseManager(this);
        getLogger().info("  ✓ DatabaseManager");
        
        this.pidController = new PidController(this);
        getLogger().info("  ✓ PidController (SoA + SIMD)");
        
        this.marketManager = new MarketManager(this);
        getLogger().info("  ✓ MarketManager (VirtualThreads)");
        
        this.integrationManager = new IntegrationManager(this);
        getLogger().info("  ✓ IntegrationManager");
    }
    
    private void registerPlaceholderAPI() {
        if (Bukkit.getPluginManager().getPlugin("PlaceholderAPI") != null) {
            new EcoBridgeExpansion(this).register();
            getLogger().info("  ✓ PlaceholderAPI integration");
        } else {
            getLogger().warning("  ⚠ PlaceholderAPI not found (variables disabled)");
        }
    }
    
    private void registerCommands() {
        if (getCommand("ecobridge") != null) {
            EcoBridgeCommand commandHandler = new EcoBridgeCommand(this);
            getCommand("ecobridge").setExecutor(commandHandler);
            getCommand("ecobridge").setTabCompleter(commandHandler);
            getLogger().info("  ✓ Commands registered (/eb, /ecobridge)");
        } else {
            getLogger().severe("  ✗ Failed to register command! Check plugin.yml");
        }
    }

    // ==================== 定时任务 ====================
    
    private void startSchedulers() {
        getLogger().info("Starting schedulers...");
        
        // [任务 1] 主计算循环 - 每 1 分钟 (1200 ticks)
        long mainInterval = getConfig().getLong("schedulers.main-loop.period", 1200L);
        long mainDelay = getConfig().getLong("schedulers.main-loop.initial-delay", 1200L);
        
        new BukkitRunnable() {
            @Override
            public void run() {
                if (!fullyInitialized) return;
                
                try {
                    // 数据采集与计算 (主线程安全)
                    integrationManager.collectDataAndCalculate();
                    
                    // 刷写缓冲区 (异步)
                    pidController.flushBuffer(false);
                    
                    // 更新活跃度系数 (需要 TPS 数据)
                    marketManager.updateActivityFactor();
                    
                } catch (Exception e) {
                    getLogger().warning("[Scheduler] Main loop error: " + e.getMessage());
                }
            }
        }.runTaskTimer(this, mainDelay, mainInterval);
        getLogger().info("  ✓ Main loop: every " + (mainInterval / 20) + "s");
        
        // [任务 2] 经济指标更新 - 每 30 分钟
        long economyInterval = getConfig().getLong("schedulers.economy-update.period", 36000L);
        long economyDelay = getConfig().getLong("schedulers.economy-update.initial-delay", 100L);
        
        new BukkitRunnable() {
            @Override
            public void run() {
                if (!fullyInitialized) return;
                
                try {
                    marketManager.updateEconomyMetrics();
                } catch (Exception e) {
                    getLogger().warning("[Scheduler] Economy update error: " + e.getMessage());
                }
            }
        }.runTaskTimerAsynchronously(this, economyDelay, economyInterval);
        getLogger().info("  ✓ Economy update: every " + (economyInterval / 20 / 60) + "min");
        
        // [任务 3] 市场波动与节假日 - 每 5 分钟
        long marketInterval = getConfig().getLong("schedulers.market-flux-update.period", 6000L);
        long marketDelay = getConfig().getLong("schedulers.market-flux-update.initial-delay", 20L);
        
        new BukkitRunnable() {
            @Override
            public void run() {
                if (!fullyInitialized) return;
                
                try {
                    marketManager.updateMarketFlux();
                    marketManager.updateHolidayCache();
                } catch (Exception e) {
                    getLogger().warning("[Scheduler] Market flux error: " + e.getMessage());
                }
            }
        }.runTaskTimerAsynchronously(this, marketDelay, marketInterval);
        getLogger().info("  ✓ Market flux: every " + (marketInterval / 20 / 60) + "min");
    }

    // ==================== 优化辅助方法 ====================
    
    /**
     * 预热关键路径 (减少首次调用延迟)
     */
    private void warmupCriticalPaths() {
        if (!getConfig().getBoolean("performance.memory-warmup", true)) return;
        
        getLogger().info("Warming up critical paths...");
        
        Thread.ofVirtual().start(() -> {
            try {
                // 等待数据库初始化完成
                int retries = 0;
                while (!databaseManager.isInitialized() && retries++ < 50) {
                    Thread.sleep(100);
                }
                
                if (databaseManager.isInitialized()) {
                    // 预热 1: 同步商店数据
                    integrationManager.syncShops();
                    getLogger().info("  ✓ Shop data synced");
                    
                    // 预热 2: 更新市场指标
                    marketManager.updateMarketFlux();
                    marketManager.updateHolidayCache();
                    getLogger().info("  ✓ Market data initialized");
                    
                    // 预热 3: 触发一次小批量计算 (激活 SIMD 代码路径)
                    // 这会让 JIT 编译器提前优化热点代码
                    getLogger().info("  ✓ Warmup complete");
                } else {
                    getLogger().warning("  ⚠ Database not ready, skipping warmup");
                }
                
            } catch (Exception e) {
                getLogger().warning("Warmup error: " + e.getMessage());
            }
        });
    }
    
    /**
     * 等待数据库刷写完成
     */
    private void waitForDatabaseFlush(long timeoutMs) {
        long startTime = System.currentTimeMillis();
        
        while (System.currentTimeMillis() - startTime < timeoutMs) {
            int pending = pidController.getDirtyQueueSize();
            if (pending == 0) {
                getLogger().info("  ✓ All data flushed to database");
                return;
            }
            
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                break;
            }
        }
        
        getLogger().warning("  ⚠ Database flush timeout, some data may be lost");
    }
    
    /**
     * 紧急关闭 (初始化失败时调用)
     */
    private void emergencyShutdown() {
        try {
            if (pidController != null) pidController.close();
            if (marketManager != null) marketManager.shutdown();
            if (databaseManager != null) databaseManager.closePool();
            if (performanceMonitor != null) performanceMonitor.shutdown();
        } catch (Exception e) {
            // 忽略清理时的异常
        }
    }

    // ==================== 公共访问器 ====================
    
    public static EcoBridge getInstance() { 
        return instance; 
    }
    
    public DatabaseManager getDatabaseManager() { 
        return databaseManager; 
    }
    
    public PidController getPidController() { 
        return pidController; 
    }
    
    public MarketManager getMarketManager() { 
        return marketManager; 
    }
    
    public IntegrationManager getIntegrationManager() { 
        return integrationManager; 
    }
    
    public PerformanceMonitor getPerformanceMonitor() {
        return performanceMonitor;
    }
    
    public boolean isFullyInitialized() {
        return fullyInitialized;
    }
}