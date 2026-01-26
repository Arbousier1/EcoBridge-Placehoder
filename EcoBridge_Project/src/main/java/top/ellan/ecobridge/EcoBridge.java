package top.ellan.ecobridge;

import org.bukkit.Bukkit;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitRunnable;

/**
 * EcoBridge - 主插件类（最终优化版）
 *
 * 已修复：
 * 1. onDisable 关闭序列（flush → 等待DB队列 → closePool → close Arena）
 * 2. waitForDatabaseManager 检查 DatabaseManager 队列深度
 * 3. 初始化顺序优化（syncShops → loadAllStates）
 * 4. PID 参数热重载支持
 * 5. warmupCriticalPaths 逻辑完善
 */
public class EcoBridge extends JavaPlugin {
    private static EcoBridge instance;

    // 核心管理器
    private DatabaseManager databaseManager;
    private PidController pidController;
    private MarketManager marketManager;
    private IntegrationManager integrationManager;

    // 性能监控器
    private PerformanceMonitor performanceMonitor;

    // 初始化状态标记
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

            // 1. 初始化核心子系统
            initializeComponents();

            // [修复] 确保 PID 配置已加载
            pidController.reloadConfig();

            // 2. 异步初始化数据库（内部会触发 loadAllStates）
            databaseManager.initPool();

            // 3. 注册 PlaceholderAPI
            registerPlaceholderAPI();

            // 4. 注册命令
            registerCommands();

            // 5. 启动定时调度器
            startSchedulers();

            // 6. 启动性能监控
            if (getConfig().getBoolean("monitoring.enabled", true)) {
                performanceMonitor = new PerformanceMonitor(this);
                getLogger().info("Performance monitoring enabled");
            }

            // 7. 预热关键路径
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
            // 1. 停止性能监控
            if (performanceMonitor != null) {
                getLogger().info("[1/6] Stopping performance monitor...");
                performanceMonitor.shutdown();
            }

            // 2. 强制刷写 PID 数据到 DatabaseManager 的 pendingWrites 队列
            if (pidController != null) {
                getLogger().info("[2/6] 将 PID 内存数据推送至数据库队列...");
                int dirty = pidController.getDirtyQueueSize();
                if (dirty > 0) {
                    getLogger().info("  → " + dirty + " 条脏数据进入队列");
                }
                pidController.flushBuffer(true);
            }

            // 3. 等待 DatabaseManager 异步写入完成
            if (databaseManager != null) {
                getLogger().info("[3/6] 等待数据库异步写入完成...");
                waitForDatabaseManager(8000);
            }

            // 4. 关闭数据库连接池
            if (databaseManager != null) {
                getLogger().info("[4/6] 关闭数据库连接池...");
                databaseManager.closePool();
            }

            // 5. 释放 FFM Arena
            if (pidController != null) {
                getLogger().info("[5/6] 释放堆外内存 Arena...");
                pidController.close();
            }

            // 6. 关闭市场管理器
            if (marketManager != null) {
                getLogger().info("[6/6] Shutting down market manager...");
                marketManager.shutdown();
            }

            // 7. 清理静态引用
            getLogger().info("[7/7] Cleaning up references...");
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

        // 主计算循环 - 每 60 秒
        long mainInterval = getConfig().getLong("schedulers.main-loop.period", 1200L);
        long mainDelay = getConfig().getLong("schedulers.main-loop.initial-delay", 1200L);

        new BukkitRunnable() {
            @Override
            public void run() {
                if (!fullyInitialized) return;
                try {
                    integrationManager.collectDataAndCalculate();
                    pidController.flushBuffer(false);
                    marketManager.updateActivityFactor();
                } catch (Exception e) {
                    getLogger().warning("[Scheduler] Main loop error: " + e.getMessage());
                }
            }
        }.runTaskTimer(this, mainDelay, mainInterval);
        getLogger().info("  ✓ Main loop: every " + (mainInterval / 20) + "s");

        // 经济指标更新 - 每 30 分钟
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

        // 市场波动与节假日 - 每 5 分钟
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
    private void warmupCriticalPaths() {
        if (!getConfig().getBoolean("performance.memory-warmup", true)) return;
        getLogger().info("Warming up critical paths...");

        Thread.ofVirtual().start(() -> {
            try {
                int retries = 0;
                while (!databaseManager.isInitialized() && retries++ < 50) {
                    Thread.sleep(100);
                }

                if (databaseManager.isInitialized()) {
                    // 先同步商店数据（保证 Handle 连续性）
                    integrationManager.syncShops();
                    getLogger().info("  ✓ Shop data synced");

                    // 再加载历史 PID 状态
                    pidController.loadAllStates();

                    // 更新市场数据
                    marketManager.updateMarketFlux();
                    marketManager.updateHolidayCache();
                    getLogger().info("  ✓ Market data initialized");

                    // 确保 PID 参数最新
                    pidController.reloadConfig();

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
     * [修复] 等待 DatabaseManager 的 pendingWrites 队列排空
     */
    private void waitForDatabaseManager(long timeoutMs) {
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < timeoutMs) {
            int pending = databaseManager.getPendingWritesCount();
            if (pending == 0) {
                getLogger().info("  ✓ 数据库队列已排空，所有数据已落盘");
                return;
            }
            getLogger().info("  → 数据库队列剩余: " + pending + " 条");
            try {
                Thread.sleep(150);
            } catch (InterruptedException e) {
                break;
            }
        }
        getLogger().warning("  ⚠ 数据库刷写超时（" + timeoutMs + "ms），可能丢失部分数据");
    }

    private void emergencyShutdown() {
        try {
            if (pidController != null) pidController.close();
            if (marketManager != null) marketManager.shutdown();
            if (databaseManager != null) databaseManager.closePool();
            if (performanceMonitor != null) performanceMonitor.shutdown();
        } catch (Exception ignored) {
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