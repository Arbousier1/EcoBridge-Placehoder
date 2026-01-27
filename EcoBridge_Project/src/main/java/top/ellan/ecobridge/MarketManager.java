package top.ellan.ecobridge;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bukkit.Bukkit;
import org.bukkit.Statistic;
import org.bukkit.entity.Player;
import org.bukkit.plugin.Plugin;
import su.nightexpress.coinsengine.CoinsEnginePlugin;
import su.nightexpress.coinsengine.api.CoinsEngineAPI;
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

public class MarketManager {
    
    private final EcoBridge plugin;
    // I/O 密集型任务 (HTTP 请求, TopManager 内存读取)
    private final ExecutorService ioExecutor = Executors.newVirtualThreadPerTaskExecutor();
    
    // 经济指标 (原子引用，无锁读写)
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
    private CoinsEnginePlugin coinsEnginePlugin;
    private Currency currency;
    
    // 配置常量
    private static final double MIN_THRESH = 100000.0;
    private static final double INF_WEIGHT = 0.3;
    private static final double PERCENTILE = 0.15; // 前 15% 的玩家作为富人基准
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
        Plugin tempPlugin = Bukkit.getPluginManager().getPlugin("CoinsEngine");
        
        // 检查插件是否存在且 API 已加载
        if (tempPlugin instanceof CoinsEnginePlugin && tempPlugin.isEnabled() && CoinsEngineAPI.isLoaded()) {
            this.coinsEnginePlugin = (CoinsEnginePlugin) tempPlugin;
            
            String currencyId = plugin.getConfig().getString("economy-settings.currency-id", "money");
            
            // 使用 CoinsEngineAPI 静态方法获取货币对象
            this.currency = CoinsEngineAPI.getCurrency(currencyId);
            
            if (this.currency != null) {
                plugin.getLogger().info("[Market] Hooked into CoinsEngine. Currency: " + currencyId);
                
                // 检查排行榜管理器是否可用
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
     * 更新经济指标 (通胀率计算)
     * 基于 CoinsEngine 内存排行榜 (Descending Sort Confirmed)
     */
    public void updateEconomyMetrics() {
        if (currency == null || coinsEnginePlugin == null) return;
        
        ioExecutor.submit(() -> {
            try {
                Optional<TopManager> topManagerOpt = coinsEnginePlugin.getTopManager();
                if (topManagerOpt.isEmpty()) return;
                
                TopManager topManager = topManagerOpt.get();
                
                // 获取排行榜 (API 保证返回已降序排列的 List)
                List<TopEntry> entries = topManager.getTopEntries(currency);
                
                if (entries.isEmpty()) return;
                
                int totalPlayers = entries.size();
                
                // 计算前 15% 的位置索引
                // index 0 是最有钱的，index (total * 0.15) 是第 15% 处的玩家
                int thresholdIndex = (int) Math.floor(totalPlayers * PERCENTILE);
                // 边界检查
                thresholdIndex = Math.max(0, Math.min(totalPlayers - 1, thresholdIndex));
                
                TopEntry thresholdEntry = entries.get(thresholdIndex);
                double threshold = Math.max(thresholdEntry.getBalance(), MIN_THRESH);
                
                // 更新原子引用
                currentThreshold.set(threshold);
                
                // 计算通胀率: 基于对数增长，避免数值爆炸
                double ratio = Math.max(1.0, threshold / MIN_THRESH);
                double newInflation = 1.0 + (Math.log(ratio) * INF_WEIGHT);
                inflation.set(newInflation);
                
                // 顺便清理旧的统计缓存
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
     * 使用 CoinsEngineAPI 静态方法获取余额 (同步方法，但通常为内存读取)
     */
    public double calculatePersonalFactor(Player player) {
        if (player.isOp()) return 1.0;
        
        double minFactor = plugin.getConfig().getDouble("personal-factor.min-factor", 0.5);
        double maxTax = plugin.getConfig().getDouble("personal-factor.rich.max-tax", 0.1);
        
        double currentFactor = 1.0;
        
        // 1. 富人税
        if (currency != null) {
            // 注意: getBalance 是同步方法。对于在线玩家，CoinsEngine 通常有内存缓存，速度很快。
            double balance = CoinsEngineAPI.getBalance(player.getUniqueId(), currency);
            double threshold = currentThreshold.get();
            
            if (balance > threshold) {
                double excess = balance - threshold;
                // 对数税率曲线
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

    // --- 辅助方法 ---

    /**
     * 高性能统计数据获取 (双重缓存机制)
     * 解决 Bukkit.getStatistic 必须在主线程调用的问题
     */
    private int getCachedStatistic(Player player, Statistic stat) {
        UUID uid = player.getUniqueId();
        Map.Entry<Integer, Long> entry = statCache.get(uid);
        long now = System.currentTimeMillis();
        
        // 1. 命中缓存且未过期 (5分钟)
        if (entry != null && (now - entry.getValue() < 300_000)) { 
            return entry.getKey();
        }
        
        // 2. 如果在主线程，直接更新并返回
        if (Bukkit.isPrimaryThread()) {
            int val = player.getStatistic(stat);
            statCache.put(uid, Map.entry(val, now));
            return val;
        } else {
            // 3. 如果在异步线程，调度主线程更新，并返回旧值(如果存在)或0
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
        // 使用时间作为种子，全服统一波动
        Random rng = new Random(currentHour + "Salt".hashCode());
        
        double range = plugin.getConfig().getDouble("market-flux.normal-range", 0.05);
        if (rng.nextDouble() < plugin.getConfig().getDouble("market-flux.event-chance", 0.1)) {
            range = plugin.getConfig().getDouble("market-flux.event-range", 0.3);
        }
        
        double rawFlux = 1.0 + ((rng.nextDouble() * 2.0 - 1.0) * range);
        marketFlux.set(Math.floor(rawFlux * inflation.get() * 10000) / 10000.0);
    }
    
    public void updateActivityFactor() {
        // 先在主线程获取 Bukkit 状态
        Bukkit.getScheduler().runTask(plugin, () -> {
            int online = Bukkit.getOnlinePlayers().size();
            double tps = Bukkit.getTPS()[0]; 
            
            // 再去异步线程计算
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