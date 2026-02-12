package top.ellan.ecobridge;

import org.bukkit.Bukkit;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitRunnable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * EcoBridge 主类 - Java 25 极致性能经济引擎
 * 
 * 【2024修正版】已添加：
 *   - TransactionListener：监听 UltimateShop 交易事件
 *   - TransactionAmountCache：存储实际交易数量
 *   - IntegrationManager 改进：读取实际数量而非次数
 */
public class EcoBridge extends JavaPlugin {

    public record MarketSnapshot(
        double totalEconomyBalance,
        int activePlayerCount,
        long timestamp,
        int[] itemHandles,
        double[] itemDeltas,
        int validItemCount
    ) {}

    public record CalculationResult(
        double globalInflationRate,
        double suggestedTaxRate
    ) {}

    private static EcoBridge instance;
    private DatabaseManager databaseManager;
    private PidController pidController;
    private MarketManager marketManager;
    private IntegrationManager integrationManager;
    private PerformanceMonitor performanceMonitor;
    private TransactionListener transactionListener;  // 【新增】交易监听器
    
    private final ExecutorService virtualExecutor = Executors.newVirtualThreadPerTaskExecutor();
    private final AtomicBoolean isEnabled = new AtomicBoolean(false);
    private final AtomicBoolean dataReady = new AtomicBoolean(false);

    @Override
    public void onEnable() {
        instance = this;
        long startTime = System.currentTimeMillis();
        printBanner();
        
        try {
            saveDefaultConfig();
            initializeComponents();
            registerPlaceholderAPI();
            registerCommands();
            registerListeners();  // 【新增】注册事件监听器
            startAsyncWarmupChain();
            startSchedulers();
            isEnabled.set(true);
            
            long elapsedMs = System.currentTimeMillis() - startTime;
            getLogger().info("═══════════════════════════════════════════════");
            getLogger().info("  ✓ EcoBridge core loaded in " + elapsedMs + "ms");
            getLogger().info("  → Using Actual Transaction Amounts (Fixed!)");
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
        isEnabled.set(false);
        dataReady.set(false);
        
        try {
            long shutdownStart = System.currentTimeMillis();
            
            if (performanceMonitor != null) performanceMonitor.shutdown();
            
            if (pidController != null) {
                getLogger().info("[1/5] Flushing PID memory buffer...");
                pidController.flushBuffer(true);
            }
            
            if (databaseManager != null) {
                getLogger().info("[2/5] Waiting for DB write queue...");
                waitForDatabaseManager(5000);
                databaseManager.closePool();
            }
            
            if (pidController != null) {
                getLogger().info("[3/5] Closing native memory arenas...");
                pidController.close();
            }
            
            if (marketManager != null) marketManager.shutdown();
            
            // 【新增】清理交易缓存
            getLogger().info("[4/5] Clearing transaction cache...");
            TransactionAmountCache.getInstance().clear();
            
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

    /**
     * 【新增】注册事件监听器
     */
    private void registerListeners() {
        // 注册交易监听器
        if (Bukkit.getPluginManager().getPlugin("UltimateShop") != null) {
            transactionListener = new TransactionListener(this);
            Bukkit.getPluginManager().registerEvents(transactionListener, this);
            getLogger().info("  ✓ TransactionListener registered (UltimateShop hooked)");
        } else {
            getLogger().warning("  ⚠ UltimateShop not found, TransactionListener not registered");
        }
    }

    private void startAsyncWarmupChain() {
        if (!getConfig().getBoolean("performance.memory-warmup", true)) {
            dataReady.set(true);
            return;
        }
        
        getLogger().info("Starting high-performance warmup chain...");
        
        CompletableFuture.runAsync(() -> {
            databaseManager.initPool();
            int retries = 0;
            while (!databaseManager.isInitialized() && retries++ < 50) {
                try { Thread.sleep(100); } catch (InterruptedException ignored) {}
            }
        }, virtualExecutor)
        .thenRunAsync(() -> {
            if (databaseManager.isInitialized()) {
                getLogger().info("  → Syncing shops (Main Thread)...");
                integrationManager.syncShops();
            }
        }, r -> Bukkit.getScheduler().runTask(this, r))
        .thenRunAsync(() -> {
            if (databaseManager.isInitialized()) {
                getLogger().info("  → Loading historical states (Virtual Thread)...");
                pidController.loadAllStates();
                pidController.reloadConfig();
                marketManager.updateMarketFlux();
                marketManager.updateHolidayCache();
            }
        }, virtualExecutor)
        .thenRun(() -> {
            dataReady.set(true);
            getLogger().info("  ✓ WARMUP COMPLETE - Engine Started");
        })
        .exceptionally(ex -> {
            getLogger().severe("Warmup chain failed: " + ex.getMessage());
            ex.printStackTrace();
            return null;
        });
    }

    private void startSchedulers() {
        long mainInterval = getConfig().getLong("schedulers.main-loop.period", 1200L);
        
        new BukkitRunnable() {
            @Override
            public void run() {
                if (!isEnabled.get() || !dataReady.get()) return;
                
                final MarketSnapshot snapshot = integrationManager.captureDataSnapshot();
                if (snapshot == null) return;
                
                virtualExecutor.submit(() -> {
                    try {
                        final CalculationResult result = pidController.calculate(snapshot);
                        pidController.flushBuffer(false);
                        
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
        try {
            EcoBridgeCommand cmd = new EcoBridgeCommand(this);
            java.lang.reflect.Field commandMapField = Bukkit.getServer().getClass().getDeclaredField("commandMap");
            commandMapField.setAccessible(true);
            org.bukkit.command.CommandMap commandMap = (org.bukkit.command.CommandMap) commandMapField.get(Bukkit.getServer());
            commandMap.register("ecobridge", cmd);
            getLogger().info("  ✓ Commands registered (Direct CommandMap Injection)");
        } catch (Exception e) {
            getLogger().severe("  ✗ Failed to register commands: " + e.getMessage());
            e.printStackTrace();
        }
    }

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
            TransactionAmountCache.getInstance().clear();
        } catch (Exception ignored) {}
    }

    private void printBanner() {
        getLogger().info("═══════════════════════════════════════════════");
        getLogger().info("  EcoBridge - Java 25 Extreme Performance");
        getLogger().info("  Architecture: Hybrid (Sync/Async) + SIMD");
        getLogger().info("  [FIXED] Using Actual Transaction Amounts");
        getLogger().info("═══════════════════════════════════════════════");
    }

    public static EcoBridge getInstance() { return instance; }
    public DatabaseManager getDatabaseManager() { return databaseManager; }
    public PidController getPidController() { return pidController; }
    public MarketManager getMarketManager() { return marketManager; }
    public IntegrationManager getIntegrationManager() { return integrationManager; }
    public PerformanceMonitor getPerformanceMonitor() { return performanceMonitor; }
    public boolean isFullyInitialized() { return dataReady.get(); }
}
