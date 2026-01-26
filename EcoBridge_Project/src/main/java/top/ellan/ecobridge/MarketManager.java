package top.ellan.ecobridge;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bukkit.Bukkit;
import org.bukkit.Statistic;
import org.bukkit.entity.Player;
import su.nightexpress.coinsengine.CoinsEnginePlugin; // 修正导入
import su.nightexpress.coinsengine.api.CoinsEngineAPI; // 修正导入
import su.nightexpress.coinsengine.api.currency.Currency;
import su.nightexpress.coinsengine.tops.TopEntry;
import su.nightexpress.coinsengine.tops.TopManager;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.DayOfWeek;
import java.time.Duration;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * MarketManager - CoinsEngine Native Optimized (Fixed)
 * 
 * 修正:
 * 1. 修复了 CoinsEngine 主类引用错误 (CoinsEngine -> CoinsEnginePlugin)
 * 2. 使用 CoinsEngineAPI 静态方法获取货币
 * 3. 保持了 Paper 异步安全性和 VirtualThread 优化
 */
public class MarketManager {
    
    private final EcoBridge plugin;
    // I/O 密集型任务 (HTTP, TopManager读取)
    private final ExecutorService ioExecutor = Executors.newVirtualThreadPerTaskExecutor();
    
    // 经济指标 (无锁引用)
    private final AtomicReference<Double> inflation = new AtomicReference<>(1.0);
    private final AtomicReference<Double> marketFlux = new AtomicReference<>(1.0);
    private final AtomicReference<Double> currentThreshold = new AtomicReference<>(100000.0);
    private final AtomicReference<Double> activityFactor = new AtomicReference<>(1.0);
    
    // 缓存
    private final ConcurrentHashMap<String, Integer> holidayCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<UUID, Map.Entry<Integer, Long>> statCache = new ConcurrentHashMap<>();
    private volatile long lastHolidayUpdate = 0;
    
    // 外部组件
    private final HttpClient httpClient;
    private final ObjectMapper jsonMapper = new ObjectMapper();
    
    // CoinsEngine 引用
    private CoinsEnginePlugin coinsEnginePlugin; // 修正类型
    private Currency currency;
    
    // 配置常量
    private static final double MIN_THRESH = 100000.0;
    private static final double INF_WEIGHT = 0.3;
    private static final double PERCENTILE = 0.15; 
    private static final String HOLIDAY_API_URL = "https://api.apihubs.cn/holiday/get";
    
    public MarketManager(EcoBridge plugin) {
        this.plugin = plugin;
        this.httpClient = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_2)
            .connectTimeout(Duration.ofSeconds(10))
            .executor(ioExecutor)
            .build();
        
        setupCoinsEngine();
    }
    
    private void setupCoinsEngine() {
        // 检查 API 是否加载
        if (Bukkit.getPluginManager().isPluginEnabled("CoinsEngine") && CoinsEngineAPI.isLoaded()) {
            this.coinsEnginePlugin = CoinsEngineAPI.plugin(); // 获取主类实例
            
            String currencyId = plugin.getConfig().getString("economy-settings.currency-id", "money");
            
            // 使用 API 静态方法获取货币
            this.currency = CoinsEngineAPI.getCurrency(currencyId);
            
            if (this.currency != null) {
                plugin.getLogger().info("[Market] Hooked into CoinsEngine. Currency: " + currencyId);
                
                // 检查排行榜管理器是否存在 (通常在主类中暴露)
                if (!coinsEnginePlugin.getTopManager().isPresent()) {
                    plugin.getLogger().warning("[Market] CoinsEngine 'Top' feature is disabled in config!");
                    plugin.getLogger().warning("[Market] Inflation calculation will be skipped.");
                }
            } else {
                plugin.getLogger().warning("[Market] Currency '" + currencyId + "' not found in CoinsEngine!");
            }
        } else {
            plugin.getLogger().warning("[Market] CoinsEngine plugin not found or API not loaded!");
        }
    }
    
    /**
     * 更新经济指标 (核心优化)
     * 利用 CoinsEngine 内存排行榜直接计算
     */
    public void updateEconomyMetrics() {
        if (currency == null || coinsEnginePlugin == null) return;
        
        ioExecutor.submit(() -> {
            try {
                // 获取 TopManager (从插件实例获取)
                Optional<TopManager> topManagerOpt = coinsEnginePlugin.getTopManager();
                
                if (topManagerOpt.isEmpty()) return; // 排行榜功能未开启
                
                TopManager topManager = topManagerOpt.get();
                
                // 获取排行榜 (内存快照，无 IO)
                List<TopEntry> entries = topManager.getTopEntries(currency);
                
                if (entries.isEmpty()) return;
                
                // 计算样本大小
                int totalPlayers = entries.size();
                
                // 计算 15% 门槛
                int thresholdIndex = (int) Math.floor(totalPlayers * PERCENTILE);
                thresholdIndex = Math.max(0, Math.min(totalPlayers - 1, thresholdIndex));
                
                TopEntry thresholdEntry = entries.get(thresholdIndex);
                double threshold = Math.max(thresholdEntry.getBalance(), MIN_THRESH);
                
                // 更新原子引用
                currentThreshold.set(threshold);
                
                // 计算通胀率
                double ratio = Math.max(1.0, threshold / MIN_THRESH);
                double newInflation = 1.0 + (Math.log(ratio) * INF_WEIGHT);
                inflation.set(newInflation);
                
                // 清理旧统计缓存
                cleanupStatCache();
                
                plugin.getLogger().info(String.format(
                    "[Market] Metrics updated via CoinsEngine: Players=%d, Thresh=%.0f, Inf=%.4f",
                    totalPlayers, threshold, newInflation
                ));
                
            } catch (Exception e) {
                plugin.getLogger().warning("[Market] Metrics update failed: " + e.getMessage());
            }
        });
    }

    /**
     * 计算个人系数 (Personal Factor)
     * 使用 CoinsEngineAPI 静态方法获取余额
     */
    public double calculatePersonalFactor(Player player) {
        if (player.isOp()) return 1.0;
        
        double minFactor = plugin.getConfig().getDouble("personal-factor.min-factor", 0.5);
        double maxTax = plugin.getConfig().getDouble("personal-factor.rich.max-tax", 0.1);
        
        double currentFactor = 1.0;
        
        // 1. 富人税 (API 调用)
        if (currency != null) {
            double balance = CoinsEngineAPI.getBalance(player.getUniqueId(), currency);
            double threshold = currentThreshold.get();
            
            if (balance > threshold) {
                double excess = balance - threshold;
                double tax = Math.log10(excess + 10) * (maxTax / 10.0);
                currentFactor += Math.min(tax, maxTax * 2);
            }
        }
        
        // 2. 老玩家折扣
        int ticksPlayed = getCachedStatistic(player, Statistic.PLAY_ONE_MINUTE);
        if (ticksPlayed > 0) {
            double vetMax = plugin.getConfig().getDouble("personal-factor.veteran.max-discount", 0.05);
            int vetHours = plugin.getConfig().getInt("personal-factor.veteran.threshold-hours", 100);
            
            double discount = (double) ticksPlayed / (vetHours * 72000L) * vetMax;
            currentFactor -= Math.min(discount, vetMax);
        }
        
        return Math.max(minFactor, currentFactor);
    }

    // --- 辅助方法 (保持不变) ---

    private int getCachedStatistic(Player player, Statistic stat) {
        UUID uid = player.getUniqueId();
        Map.Entry<Integer, Long> entry = statCache.get(uid);
        long now = System.currentTimeMillis();
        
        if (entry != null && (now - entry.getValue() < 300_000)) { 
            return entry.getKey();
        }
        
        if (Bukkit.isPrimaryThread()) {
            int val = player.getStatistic(stat);
            statCache.put(uid, Map.entry(val, now));
            return val;
        } else {
            Bukkit.getScheduler().runTask(plugin, () -> {
                if (player.isOnline()) {
                    statCache.put(uid, Map.entry(player.getStatistic(stat), System.currentTimeMillis()));
                }
            });
            return entry != null ? entry.getKey() : 0;
        }
    }
    
    private void cleanupStatCache() {
        long now = System.currentTimeMillis();
        statCache.entrySet().removeIf(e -> (now - e.getValue().getValue() > 600_000));
    }

    public void updateMarketFlux() {
        long currentHour = System.currentTimeMillis() / 3600000L;
        Random rng = new Random(currentHour + "Salt".hashCode());
        
        double range = plugin.getConfig().getDouble("market-flux.normal-range", 0.05);
        if (rng.nextDouble() < plugin.getConfig().getDouble("market-flux.event-chance", 0.1)) {
            range = plugin.getConfig().getDouble("market-flux.event-range", 0.3);
        }
        
        double rawFlux = 1.0 + ((rng.nextDouble() * 2.0 - 1.0) * range);
        marketFlux.set(Math.floor(rawFlux * inflation.get() * 10000) / 10000.0);
    }
    
    public void updateActivityFactor() {
        Bukkit.getScheduler().runTask(plugin, () -> {
            int online = Bukkit.getOnlinePlayers().size();
            double tps = Bukkit.getTPS()[0]; 
            
            ioExecutor.submit(() -> {
                double base = plugin.getConfig().getDouble("activity.base", 1.0);
                double impact = plugin.getConfig().getDouble("activity.player-impact", 0.001);
                double factor = base + (online * impact);
                
                double tpsLimit = plugin.getConfig().getDouble("activity.tps-limit", 18.0);
                if (tps < tpsLimit) {
                    double penalty = (20.0 - tps) * plugin.getConfig().getDouble("activity.tps-weight", 0.05);
                    factor = Math.max(0.1, factor - penalty);
                }
                
                activityFactor.set(factor);
            });
        });
    }

    public void updateHolidayCache() {
        if (System.currentTimeMillis() - lastHolidayUpdate < 86400000) return;
        
        int year = LocalDate.now().getYear();
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(HOLIDAY_API_URL + "?year=" + year))
            .header("User-Agent", "EcoBridge/Paper")
            .timeout(Duration.ofSeconds(5))
            .GET()
            .build();
            
        httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
            .thenAccept(res -> {
                if (res.statusCode() == 200) {
                    try {
                        JsonNode root = jsonMapper.readTree(res.body());
                        if (root.path("data").isArray()) {
                            holidayCache.clear();
                            for (JsonNode node : root.get("data")) {
                                holidayCache.put(node.get("date").asText(), node.get("type").asInt());
                            }
                            lastHolidayUpdate = System.currentTimeMillis();
                        }
                    } catch (Exception ignored) {}
                }
            });
    }
    
    public double getHolidayFactor() {
        String dateKey = LocalDate.now().toString();
        Integer type = holidayCache.get(dateKey);
        
        if (type != null) {
            return switch (type) {
                case 0 -> plugin.getConfig().getDouble("holidays.workday", 1.0);
                case 1 -> plugin.getConfig().getDouble("holidays.weekend", 1.1);
                case 2 -> plugin.getConfig().getDouble("holidays.holiday", 1.5);
                default -> 1.0;
            };
        }
        
        DayOfWeek day = LocalDate.now().getDayOfWeek();
        return (day == DayOfWeek.SATURDAY || day == DayOfWeek.SUNDAY) 
            ? plugin.getConfig().getDouble("holidays.weekend", 1.1) 
            : 1.0;
    }

    public void shutdown() {
        ioExecutor.shutdownNow();
    }

    public double getInflation() { return inflation.get(); }
    public double getMarketFlux() { return marketFlux.get(); }
    public double getActivity() { return activityFactor.get(); }
    public double getCurrentThreshold() { return currentThreshold.get(); }
}