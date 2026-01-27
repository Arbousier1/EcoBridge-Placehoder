package top.ellan.ecobridge;

import org.bukkit.Bukkit;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitRunnable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * EcoBridge 主类 - 最终架构版
 * 架构核心: Java 21+ Virtual Threads + SIMD + "Snapshot-Compute-Apply" Model
 */
public class EcoBridge extends JavaPlugin {
    
    // ==================== 数据传输对象 (DTO Records) ====================
    // 必须定义在此处，供 IntegrationManager 和 PidController 使用
    public record MarketSnapshot(
        double totalEconomyBalance, 
        int activePlayerCount, 
        long timestamp,
        int[] itemHandles,      // 原始数组 (SIMD 友好)
        double[] itemDeltas,    // 原始数组 (SIMD 友好)
        int validItemCount      // 实际有效数据量
    ) {}

    public record CalculationResult(
        double globalInflationRate, 
        double suggestedTaxRate
    ) {}

    // ==================== 静态实例 ====================
    private static EcoBridge instance;

    // ==================== 核心组件 ====================
    private DatabaseManager databaseManager;
    private PidController pidController;
    private MarketManager marketManager;
    private IntegrationManager integrationManager;
    private PerformanceMonitor performanceMonitor;

    // ==================== 并发控制 ====================
    // 虚拟线程池：用于处理 PID 密集计算和 I/O
    private final ExecutorService virtualExecutor = Executors.newVirtualThreadPerTaskExecutor();
    
    // 状态原子标记
    private final AtomicBoolean isEnabled = new AtomicBoolean(false);
    private final AtomicBoolean dataReady = new AtomicBoolean(false);

    @Override
    public void onEnable() {
        instance = this;
        long startTime = System.currentTimeMillis();

        printBanner();

        try {
            // 1. 基础初始化
            saveDefaultConfig();
            initializeComponents();

            // 2. 注册 Bukkit 钩子
            registerPlaceholderAPI();
            registerCommands();

            // 3. 启动高性能预热链 (非阻塞)
            // 这将按顺序初始化 DB -> 同步商店 -> 加载 PID 状态
            startAsyncWarmupChain();

            // 4. 启动任务调度器 (任务内部会检查 dataReady 标记)
            startSchedulers();

            // 5. 标记插件已启用
            isEnabled.set(true);

            long elapsedMs = System.currentTimeMillis() - startTime;
            getLogger().info("═══════════════════════════════════════════════");
            getLogger().info("  ✓ EcoBridge core loaded in " + elapsedMs + "ms");
            getLogger().info("  → Waiting for async warmup to complete...");
            getLogger().info("═══════════════════════════════════════════════");

        } catch (Exception e) {
            getLogger().severe("  ✗ FATAL: Failed to initialize EcoBridge");
            e.printStackTrace();
            emergencyShutdown();
            Bukkit.getPluginManager().disablePlugin(this);
        }
    }

    @Override
    public void onDisable() {
        getLogger().info("Stopping EcoBridge...");
        
        // 1. 立即停止新的计算任务
        isEnabled.set(false);
        dataReady.set(false);

        try {
            long shutdownStart = System.currentTimeMillis();

            // 2. 停止性能监控
            if (performanceMonitor != null) performanceMonitor.shutdown();

            // 3. 强制刷写 PID 内存缓冲区
            if (pidController != null) {
                getLogger().info("[1/4] Flushing PID memory buffer...");
                pidController.flushBuffer(true);
            }

            // 4. 等待数据库写入完成 (防止数据丢失)
            if (databaseManager != null) {
                getLogger().info("[2/4] Waiting for DB write queue...");
                waitForDatabaseManager(5000); // 最多等待 5秒
                databaseManager.closePool();
            }

            // 5. 释放堆外内存 (FFM Arena)
            if (pidController != null) {
                getLogger().info("[3/4] Closing native memory arenas...");
                pidController.close();
            }

            // 6. 关闭其他管理器
            if (marketManager != null) marketManager.shutdown();
            
            // 7. 关闭虚拟线程池
            virtualExecutor.shutdown();

            long elapsed = System.currentTimeMillis() - shutdownStart;
            getLogger().info("  ✓ Shutdown complete in " + elapsed + "ms");

        } catch (Exception e) {
            getLogger().severe("Error during shutdown: " + e.getMessage());
            e.printStackTrace();
        } finally {
            instance = null;
        }
    }

    // ==================== 核心逻辑: 异步预热链 ====================
    private void startAsyncWarmupChain() {
        // 如果配置跳过预热
        if (!getConfig().getBoolean("performance.memory-warmup", true)) {
            dataReady.set(true);
            return;
        }

        getLogger().info("Starting high-performance warmup chain...");

        // 步骤 1: (Virtual Thread) 初始化 DB 连接池
        CompletableFuture.runAsync(() -> {
            databaseManager.initPool();
            // 简单的自旋等待 HikariCP 就绪
            int retries = 0;
            while (!databaseManager.isInitialized() && retries++ < 50) {
                try { Thread.sleep(100); } catch (InterruptedException ignored) {}
            }
        }, virtualExecutor)

        // 步骤 2: (Main Thread) 同步 Bukkit 数据 (商店/经济)
        // 必须切回主线程，否则触发 AsyncCatcher 报错
        .thenRunAsync(() -> {
            if (databaseManager.isInitialized()) {
                getLogger().info("  → Syncing shops (Main Thread)...");
                integrationManager.syncShops(); 
            }
        }, r -> Bukkit.getScheduler().runTask(this, r))

        // 步骤 3: (Virtual Thread) 加载历史数据与 PID 状态
        .thenRunAsync(() -> {
            if (databaseManager.isInitialized()) {
                getLogger().info("  → Loading historical states (Virtual Thread)...");
                pidController.loadAllStates();
                pidController.reloadConfig();
                marketManager.updateMarketFlux();
                marketManager.updateHolidayCache();
            }
        }, virtualExecutor)

        // 步骤 4: 完成，打开计算闸门
        .thenRun(() -> {
            dataReady.set(true);
            getLogger().info("  ✓ WARMUP COMPLETE - Engine Started");
        })
        
        // 异常处理
        .exceptionally(ex -> {
            getLogger().severe("Warmup chain failed: " + ex.getMessage());
            ex.printStackTrace();
            return null;
        });
    }

    // ==================== 任务调度器 (Snapshot-Compute-Apply 模型) ====================
    private void startSchedulers() {
        long mainInterval = getConfig().getLong("schedulers.main-loop.period", 1200L);

        // 1. 主计算循环 (Pipeline)
        new BukkitRunnable() {
            @Override
            public void run() {
                // 如果未启用或数据未就绪，跳过
                if (!isEnabled.get() || !dataReady.get()) return;

                // [Phase 1: Sync] 极速快照采集 (Main Thread, <50us)
                final MarketSnapshot snapshot = integrationManager.captureDataSnapshot();
                if (snapshot == null) return;

                // [Phase 2: Async] 提交给虚拟线程计算 (Off-Main Thread)
                virtualExecutor.submit(() -> {
                    try {
                        // PID 计算 (Heavy Math / SIMD)
                        final CalculationResult result = pidController.calculate(snapshot);
                        
                        // 顺便异步刷写一次缓冲区
                        pidController.flushBuffer(false);

                        // [Phase 3: Sync] 应用结果到游戏逻辑 (Main Thread)
                        Bukkit.getScheduler().runTask(EcoBridge.this, () -> {
                            if (!isEnabled.get()) return;
                            marketManager.applyCalculationResult(result);
                        });
                    } catch (Exception e) {
                        getLogger().warning("Async calculation error: " + e.getMessage());
                    }
                });
            }
        }.runTaskTimer(this, mainInterval, mainInterval);

        // 2. 经济指标更新 (纯异步)
        new BukkitRunnable() {
            @Override
            public void run() {
                if (!isEnabled.get() || !dataReady.get()) return;
                try {
                    marketManager.updateEconomyMetrics();
                } catch (Exception e) {
                    getLogger().warning("Economy update error: " + e.getMessage());
                }
            }
        }.runTaskTimerAsynchronously(this, 100L, getConfig().getLong("schedulers.economy-update.period", 36000L));

        // 3. 市场波动更新 (纯异步)
        new BukkitRunnable() {
            @Override
            public void run() {
                if (!isEnabled.get() || !dataReady.get()) return;
                try {
                    marketManager.updateMarketFlux();
                    marketManager.updateHolidayCache();
                } catch (Exception e) {
                    getLogger().warning("Flux update error: " + e.getMessage());
                }
            }
        }.runTaskTimerAsynchronously(this, 20L, getConfig().getLong("schedulers.market-flux-update.period", 6000L));
    }

    // ==================== 辅助方法 ====================
    private void initializeComponents() {
        this.databaseManager = new DatabaseManager(this);
        this.pidController = new PidController(this);
        this.marketManager = new MarketManager(this);
        this.integrationManager = new IntegrationManager(this);
        
        if (getConfig().getBoolean("monitoring.enabled", true)) {
            this.performanceMonitor = new PerformanceMonitor(this);
        }
    }

    private void registerPlaceholderAPI() {
        if (Bukkit.getPluginManager().getPlugin("PlaceholderAPI") != null) {
            new EcoBridgeExpansion(this).register();
            getLogger().info("  ✓ PlaceholderAPI hooked");
        }
    }

    private void registerCommands() {
        if (getCommand("ecobridge") != null) {
            EcoBridgeCommand cmd = new EcoBridgeCommand(this);
            getCommand("ecobridge").setExecutor(cmd);
            getCommand("ecobridge").setTabCompleter(cmd);
        }
    }

    /**
     * 安全等待 DB 队列排空
     */
    private void waitForDatabaseManager(long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (databaseManager.getPendingWritesCount() == 0) return;
            try { 
                Thread.sleep(50); 
            } catch (InterruptedException e) { 
                Thread.currentThread().interrupt(); 
                break; 
            }
        }
        getLogger().warning("  ⚠ DB Flush Timeout: Proceeding with shutdown anyway.");
    }
    
    private void emergencyShutdown() {
        try {
            if (pidController != null) pidController.close();
            if (marketManager != null) marketManager.shutdown();
            if (databaseManager != null) databaseManager.closePool();
            if (performanceMonitor != null) performanceMonitor.shutdown();
        } catch (Exception ignored) {}
    }

    private void printBanner() {
        getLogger().info("═══════════════════════════════════════════════");
        getLogger().info("  EcoBridge - Java 25 Extreme Performance");
        getLogger().info("  Architecture: Hybrid (Sync/Async) + SIMD");
        getLogger().info("═══════════════════════════════════════════════");
    }

    // ==================== Getters ====================
    public static EcoBridge getInstance() { return instance; }
    public DatabaseManager getDatabaseManager() { return databaseManager; }
    public PidController getPidController() { return pidController; }
    public MarketManager getMarketManager() { return marketManager; }
    public IntegrationManager getIntegrationManager() { return integrationManager; }
    public PerformanceMonitor getPerformanceMonitor() { return performanceMonitor; }
    public boolean isFullyInitialized() { return dataReady.get(); }
}