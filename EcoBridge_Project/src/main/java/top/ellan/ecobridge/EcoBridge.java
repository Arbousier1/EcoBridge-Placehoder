package top.ellan.ecobridge;

import org.bukkit.Bukkit;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitRunnable;

public class EcoBridge extends JavaPlugin {
    private static EcoBridge instance;

    // 核心管理器
    private DatabaseManager databaseManager;
    private PidController pidController;
    private MarketManager marketManager;
    private IntegrationManager integrationManager;
    private WebManager webManager;

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

            // 1. 初始化核心子系统 (Database, PID, Market, Web, Integration)
            initializeComponents();

            // [配置] 确保 PID 配置已加载
            if (pidController != null) pidController.reloadConfig();

            // 2. 异步初始化数据库 (内部会自动触发 loadAllStates 加载历史数据)
            if (databaseManager != null) databaseManager.initPool();

            // 3. === [关键修复] 先启动性能监控，再启动调度器 ===
            // 必须放在 startSchedulers() 之前，防止 NPE
            if (getConfig().getBoolean("monitoring.enabled", true)) {
                // 传入 this 和 pidController
                performanceMonitor = new PerformanceMonitor(this, pidController);
                getLogger().info("  ✓ Performance monitoring enabled");
            } else {
                getLogger().info("  - Performance monitoring disabled in config");
            }

            // 4. 注册 PlaceholderAPI
            registerPlaceholderAPI();

            // 5. 注册命令
            registerCommands();

            // 6. 启动定时调度器 (此时 performanceMonitor 已不为 null)
            startSchedulers();

            // 7. 预热关键路径 (包含异步等待数据库和同步回调主线程)
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

        fullyInitialized = false; // 立即停止调度器中的逻辑执行

        try {
            // 1. 停止性能监控
            if (performanceMonitor != null) {
                getLogger().info("[1/7] Stopping performance monitor...");
                performanceMonitor.shutdown();
            }

            // 2. 强制刷写 PID 数据到 DatabaseManager 的 pendingWrites 队列
            if (pidController != null) {
                getLogger().info("[2/7] 将 PID 内存数据推送至数据库队列...");
                int dirty = pidController.getDirtyQueueSize();
                if (dirty > 0) {
                    getLogger().info("  → " + dirty + " 条脏数据进入队列");
                }
                pidController.flushBuffer(true);
            }

            // 3. 等待 DatabaseManager 异步写入完成
            if (databaseManager != null) {
                getLogger().info("[3/7] 等待数据库异步写入完成...");
                waitForDatabaseManager(8000);
            }

            // 4. 关闭数据库连接池 (包含最终队列排空)
            if (databaseManager != null) {
                getLogger().info("[4/7] 关闭数据库连接池...");
                databaseManager.closePool();
            }

            // 5. 释放 FFM Arena (堆外内存)
            if (pidController != null) {
                getLogger().info("[5/7] 释放堆外内存 Arena...");
                pidController.close();
            }

            // 6. 关闭 Web 管理器
            if (webManager != null) {
                getLogger().info("[6/7] Shutting down WebManager...");
                webManager.shutdown();
            }

            // 7. 关闭市场管理器
            if (marketManager != null) {
                getLogger().info("[7/7] Shutting down market manager...");
                marketManager.shutdown();
            }

            instance = null;

            getLogger().info("═══════════════════════════════════════════════");
            getLogger().info("  ✓ EcoBridge shutdown complete");
            getLogger().info("═══════════════════════════════════════════════");

        } catch (Exception e) {
            getLogger().severe("Error during shutdown: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // ==================== 初始化辅助方法 ====================

    private void initializeComponents() {
        getLogger().info("Initializing core components...");

        this.databaseManager = new DatabaseManager(this);
        getLogger().info("  ✓ DatabaseManager");

        this.pidController = new PidController(this);
        getLogger().info("  ✓ PidController");

        this.marketManager = new MarketManager(this);
        getLogger().info("  ✓ MarketManager");

        this.integrationManager = new IntegrationManager(this);
        getLogger().info("  ✓ IntegrationManager");

        this.webManager = new WebManager(this);
        getLogger().info("  ✓ WebManager");
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

    // ==================== 核心调度器 ====================
    private void startSchedulers() {
        getLogger().info("Starting schedulers...");

        long mainInterval = getConfig().getLong("schedulers.main-loop.period", 1200L);
        long mainDelay = getConfig().getLong("schedulers.main-loop.initial-delay", 1200L);

        // [主循环] 必须同步执行！
        // 原因：integrationManager.collectDataAndCalculate() 涉及 Bukkit Inventory 操作
        new BukkitRunnable() {
            @Override
            public void run() {
                if (!fullyInitialized) return;
                try {
                    integrationManager.collectDataAndCalculate();
                    if (pidController != null) pidController.flushBuffer(false); // 异步 I/O，但提交操作轻量
                    if (marketManager != null) marketManager.updateActivityFactor();
                } catch (Exception e) {
                    getLogger().warning("[Scheduler] Main loop error: " + e.getMessage());
                }
            }
        }.runTaskTimer(this, mainDelay, mainInterval);
        getLogger().info("  ✓ Main loop: every " + (mainInterval / 20) + "s");

        // [经济更新] 
        long economyInterval = getConfig().getLong("schedulers.economy-update.period", 36000L);
        long economyDelay = getConfig().getLong("schedulers.economy-update.initial-delay", 100L);

        new BukkitRunnable() {
            @Override
            public void run() {
                if (!fullyInitialized) return;
                try {
                    if (marketManager != null) marketManager.updateEconomyMetrics();
                } catch (Exception e) {
                    getLogger().warning("[Scheduler] Economy update error: " + e.getMessage());
                }
            }
        }.runTaskTimerAsynchronously(this, economyDelay, economyInterval);
        getLogger().info("  ✓ Economy update: every " + (economyInterval / 20 / 60) + "min");

        // [市场波动] 纯数值计算 + HTTP 请求，异步安全
        long marketInterval = getConfig().getLong("schedulers.market-flux-update.period", 6000L);
        long marketDelay = getConfig().getLong("schedulers.market-flux-update.initial-delay", 20L);

        new BukkitRunnable() {
            @Override
            public void run() {
                if (!fullyInitialized) return;
                try {
                    if (marketManager != null) {
                        marketManager.updateMarketFlux();
                        marketManager.updateHolidayCache();
                    }
                } catch (Exception e) {
                    getLogger().warning("[Scheduler] Market flux error: " + e.getMessage());
                }
            }
        }.runTaskTimerAsynchronously(this, marketDelay, marketInterval);
        getLogger().info("  ✓ Market flux: every " + (marketInterval / 20 / 60) + "min");

        // [Web 报表] 纯 HTTP 推送，异步安全
        long reportInterval = 1200L; // 60s
        long reportDelay = 1200L;    // 延迟 60s 启动，防止刚开服数据不准

        new BukkitRunnable() {
            @Override
            public void run() {
                // 这里加个双重保险，防止 task 刚启动时变量仍为 null
                if (!fullyInitialized || performanceMonitor == null || webManager == null) return;
                
                try {
                    // 获取数据
                    var stats = performanceMonitor.getCurrentStats();
                    double inflation = (marketManager != null) ? marketManager.getInflation() : 1.0;

                    // 推送
                    webManager.pushMetrics(inflation, stats);
                } catch (Exception e) {
                    getLogger().warning("[Scheduler] Report push error: " + e.getMessage());
                }
            }
        }.runTaskTimerAsynchronously(this, reportDelay, reportInterval);
        getLogger().info("  ✓ Report sampling: every " + (reportInterval / 20) + "s");
    }

    // ==================== 关键路径预热 ====================
    private void warmupCriticalPaths() {
        if (!getConfig().getBoolean("performance.memory-warmup", true)) return;
        getLogger().info("Warming up critical paths...");

        // 使用虚拟线程等待数据库，避免阻塞主线程
        Thread.ofVirtual().start(() -> {
            try {
                // 1. 自旋等待数据库连接池初始化
                int retries = 0;
                while (databaseManager != null && !databaseManager.isInitialized() && retries++ < 50) {
                    Thread.sleep(100);
                }

                if (databaseManager != null && databaseManager.isInitialized()) {
                    // 2. [重要修复] 同步商店数据必须回到主线程执行
                    Bukkit.getScheduler().runTask(this, () -> {
                        try {
                            if (integrationManager != null) {
                                integrationManager.syncShops();
                                getLogger().info("  ✓ Shop data synced (Main Thread)");
                            }
                        } catch (Exception e) {
                            getLogger().warning("Shop sync error: " + e.getMessage());
                        }
                    });

                    // 3. 更新市场与节假日数据 (HTTP 请求，可在虚拟线程执行)
                    if (marketManager != null) {
                        marketManager.updateMarketFlux();
                        marketManager.updateHolidayCache();
                        getLogger().info("  ✓ Market data initialized");
                    }

                    // 4. 再次刷新配置，确保一致性
                    if (pidController != null) pidController.reloadConfig();

                    getLogger().info("  ✓ Warmup complete");
                } else {
                    getLogger().warning("  ⚠ Database not ready, skipping warmup");
                }
            } catch (Exception e) {
                getLogger().warning("Warmup error: " + e.getMessage());
            }
        });
    }

    // ==================== 关服等待逻辑 ====================
    private void waitForDatabaseManager(long timeoutMs) {
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < timeoutMs) {
            int pending = databaseManager.getPendingWritesCount();
            if (pending == 0) {
                getLogger().info("  ✓ 数据库队列已排空，所有数据已落盘");
                return;
            }
            // 每 200ms 输出一次日志，避免刷屏
            if ((System.currentTimeMillis() - startTime) % 1000 < 200) {
                 getLogger().info("  → 数据库队列剩余: " + pending + " 条");
            }
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
            if (webManager != null) webManager.shutdown();
        } catch (Exception ignored) {
        }
    }

    // ==================== Getters ====================
    public static EcoBridge getInstance() { return instance; }
    public DatabaseManager getDatabaseManager() { return databaseManager; }
    public PidController getPidController() { return pidController; }
    public MarketManager getMarketManager() { return marketManager; }
    public IntegrationManager getIntegrationManager() { return integrationManager; }
    public PerformanceMonitor getPerformanceMonitor() { return performanceMonitor; }
    public WebManager getWebManager() { return webManager; }
    public boolean isFullyInitialized() { return fullyInitialized; }
}