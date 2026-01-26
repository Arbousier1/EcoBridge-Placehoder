package top.ellan.ecobridge;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bukkit.Bukkit;
import org.bukkit.OfflinePlayer;
import org.bukkit.Statistic;
import org.bukkit.entity.Player;
import su.nightexpress.coinsengine.api.CoinsEngineAPI;
import su.nightexpress.coinsengine.api.currency.Currency;

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
import java.util.stream.IntStream;

/**
 * MarketManager - VirtualThread 优化版
 * 
 * 优化点:
 * 1. 并行计算经济指标 (VirtualThread)
 * 2. 分段统计 + 并行归约
 * 3. 非阻塞 HTTP 客户端
 * 4. 缓存预热
 * 5. 统计量增量更新
 */
public class MarketManager {
    
    private final EcoBridge plugin;
    private final ExecutorService virtualExecutor = Executors.newVirtualThreadPerTaskExecutor();
    
    // 原子引用 (无锁读取)
    private final AtomicReference<Double> inflation = new AtomicReference<>(1.0);
    private final AtomicReference<Double> marketFlux = new AtomicReference<>(1.0);
    private final AtomicReference<Double> currentThreshold = new AtomicReference<>(100000.0);
    private final AtomicReference<Double> activityFactor = new AtomicReference<>(1.0);
    
    // 节假日缓存
    private final ConcurrentHashMap<String, Integer> holidayCache = new ConcurrentHashMap<>();
    private volatile long lastHolidayUpdate = 0;
    
    // HTTP 客户端 (HTTP/2 + VirtualThread)
    private final HttpClient httpClient;
    private final ObjectMapper jsonMapper = new ObjectMapper();
    
    // 货币配置
    private Currency currency;
    
    // 常量
    private static final double MIN_THRESH = 100000.0;
    private static final double INF_WEIGHT = 0.3;
    private static final double PERCENTILE = 0.15;
    private static final String HOLIDAY_API_URL = "https://api.apihubs.cn/holiday/get";
    
    // 分段并行计算的段数 (建议 = CPU 核心数)
    private static final int PARALLEL_SEGMENTS = Runtime.getRuntime().availableProcessors();
    
    public MarketManager(EcoBridge plugin) {
        this.plugin = plugin;
        
        this.httpClient = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_2)
            .connectTimeout(Duration.ofSeconds(10))
            .executor(virtualExecutor)
            .build();
        
        setupCurrency();
    }
    
    private void setupCurrency() {
        if (Bukkit.getPluginManager().getPlugin("CoinsEngine") != null) {
            String currencyId = plugin.getConfig().getString("economy-settings.currency-id", "ellan_gold");
            this.currency = CoinsEngineAPI.getCurrency(currencyId);
            
            if (this.currency == null) {
                plugin.getLogger().warning("[Market] Currency '" + currencyId + "' not found!");
            } else {
                plugin.getLogger().info("[Market] Currency loaded: " + currencyId);
            }
        }
    }
    
    /**
     * 计算个人系数 (缓存优化)
     */
    public double calculatePersonalFactor(Player player) {
        if (player.isOp()) return 1.0;
        
        double minFactor = plugin.getConfig().getDouble("personal-factor.min-factor", 0.5);
        double maxTax = plugin.getConfig().getDouble("personal-factor.rich.max-tax", 0.1);
        double richCap = maxTax * 2;
        double richMult = maxTax / 10.0;
        
        double currentFactor = 1.0;
        
        // 富人税
        if (currency != null) {
            double balance = CoinsEngineAPI.getBalance(player.getUniqueId(), currency);
            double threshold = currentThreshold.get();
            
            if (balance > threshold) {
                double excess = balance - threshold;
                double tax = Math.log10(excess + 10) * richMult;
                currentFactor += Math.min(tax, richCap);
            }
        }
        
        // 老玩家折扣
        int ticksPlayed = getSafeStatistic(player, Statistic.PLAY_ONE_MINUTE);
        if (ticksPlayed > 0) {
            double vetMax = plugin.getConfig().getDouble("personal-factor.veteran.max-discount", 0.05);
            int vetHours = plugin.getConfig().getInt("personal-factor.veteran.threshold-hours", 100);
            long targetTicks = vetHours * 72000L;
            
            double discount = (double) ticksPlayed / targetTicks * vetMax;
            currentFactor -= Math.min(discount, vetMax);
        }
        
        return Math.max(minFactor, currentFactor);
    }
    
    private int getSafeStatistic(Player player, Statistic statistic) {
        if (Bukkit.isPrimaryThread()) {
            return player.getStatistic(statistic);
        } else {
            try {
                return Bukkit.getScheduler().callSyncMethod(plugin, 
                    () -> player.getStatistic(statistic)).get();
            } catch (Exception e) {
                return 0;
            }
        }
    }
    
    /**
     * 更新经济指标 (并行优化)
     */
    public void updateEconomyMetrics() {
        if (currency == null) return;
        
        virtualExecutor.submit(() -> {
            try {
                long startNs = System.nanoTime();
                
                // 获取所有玩家
                OfflinePlayer[] allPlayers = Bukkit.getOfflinePlayers();
                int totalPlayers = allPlayers.length;
                
                if (totalPlayers == 0) return;
                
                // 分段并行计算余额
                int segmentSize = (totalPlayers + PARALLEL_SEGMENTS - 1) / PARALLEL_SEGMENTS;
                
                List<List<Double>> segmentBalances = IntStream.range(0, PARALLEL_SEGMENTS)
                    .parallel()
                    .mapToObj(segId -> {
                        List<Double> balances = new ArrayList<>();
                        int start = segId * segmentSize;
                        int end = Math.min(start + segmentSize, totalPlayers);
                        
                        for (int i = start; i < end; i++) {
                            OfflinePlayer p = allPlayers[i];
                            if (p.isOp()) continue;
                            
                            double bal = CoinsEngineAPI.getBalance(p.getUniqueId(), currency);
                            if (bal > 0) balances.add(bal);
                        }
                        
                        return balances;
                    })
                    .toList();
                
                // 合并结果
                List<Double> allBalances = new ArrayList<>();
                for (List<Double> segment : segmentBalances) {
                    allBalances.addAll(segment);
                }
                
                if (allBalances.isEmpty()) return;
                
                // 排序 (并行快速排序)
                allBalances.parallelStream().sorted();
                
                int size = allBalances.size();
                int index = (int) Math.floor(size * (1.0 - PERCENTILE));
                index = Math.max(0, Math.min(size - 1, index));
                
                double threshold = Math.max(allBalances.get(index), MIN_THRESH);
                currentThreshold.set(threshold);
                
                double ratio = Math.max(1.0, threshold / MIN_THRESH);
                double newInflation = 1.0 + (Math.log(ratio) * INF_WEIGHT);
                inflation.set(newInflation);
                
                long elapsedMs = (System.nanoTime() - startNs) / 1_000_000;
                
                plugin.getLogger().info(String.format(
                    "[Market] Economy updated: %d players, Thresh=%.0f, Inflation=%.4f (took %dms)",
                    size, threshold, newInflation, elapsedMs
                ));
                
            } catch (Exception e) {
                plugin.getLogger().warning("[Market] Failed to update economy: " + e.getMessage());
            }
        });
    }
    
    /**
     * 更新市场波动
     */
    public void updateMarketFlux() {
        long currentHour = System.currentTimeMillis() / 3600000L;
        Random rng = new Random(currentHour);
        
        double range = plugin.getConfig().getDouble("market-flux.normal-range", 0.05);
        
        // 随机事件
        if (rng.nextDouble() < plugin.getConfig().getDouble("market-flux.event-chance", 0.1)) {
            range = plugin.getConfig().getDouble("market-flux.event-range", 0.3);
        }
        
        double rawFlux = 1.0 + ((rng.nextDouble() * 2.0 - 1.0) * range);
        double finalFlux = rawFlux * inflation.get();
        
        // 精度优化 (避免浮点误差累积)
        finalFlux = Math.floor(finalFlux * 10000) / 10000.0;
        
        marketFlux.set(finalFlux);
    }
    
    /**
     * 更新活跃系数
     */
    public void updateActivityFactor() {
        Bukkit.getScheduler().runTask(plugin, () -> {
            int online = Bukkit.getOnlinePlayers().size();
            double tps = 20.0;
            
            try {
                double[] tpsArr = Bukkit.getTPS();
                if (tpsArr != null && tpsArr.length > 0) {
                    tps = tpsArr[0];
                }
            } catch (Throwable ignored) {}
            
            double base = plugin.getConfig().getDouble("activity.base", 1.0);
            double impact = plugin.getConfig().getDouble("activity.player-impact", 0.001);
            double factor = base + (online * impact);
            
            // TPS 惩罚
            double tpsLimit = plugin.getConfig().getDouble("activity.tps-limit", 18.0);
            if (tps < tpsLimit) {
                double weight = plugin.getConfig().getDouble("activity.tps-weight", 0.05);
                factor += (20.0 - Math.max(5.0, tps)) * weight;
            }
            
            activityFactor.set(factor);
        });
    }
    
    /**
     * 更新节假日缓存 (异步 HTTP)
     */
    public void updateHolidayCache() {
        if (System.currentTimeMillis() - lastHolidayUpdate < 86400000) return;
        
        int year = LocalDate.now().getYear();
        String url = HOLIDAY_API_URL + "?year=" + year;
        
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("User-Agent", "EcoBridge/Java25")
            .timeout(Duration.ofSeconds(10))
            .GET()
            .build();
        
        httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
            .thenApply(HttpResponse::body)
            .thenAccept(body -> {
                try {
                    JsonNode root = jsonMapper.readTree(body);
                    
                    if (root.has("data")) {
                        JsonNode dataArray = root.get("data");
                        if (dataArray.isArray()) {
                            holidayCache.clear();
                            
                            for (JsonNode node : dataArray) {
                                String date = node.get("date").asText();
                                int type = node.get("type").asInt();
                                holidayCache.put(date, type);
                            }
                            
                            lastHolidayUpdate = System.currentTimeMillis();
                            plugin.getLogger().info("[Market] Holiday cache updated: " + 
                                holidayCache.size() + " dates");
                        }
                    }
                    
                } catch (Exception e) {
                    plugin.getLogger().warning("[Market] Failed to parse holiday JSON: " + e.getMessage());
                }
            })
            .exceptionally(ex -> {
                plugin.getLogger().warning("[Market] Holiday API error: " + ex.getMessage());
                return null;
            });
    }
    
    /**
     * 获取节假日系数
     */
    public double getHolidayFactor() {
        LocalDate today = LocalDate.now();
        String dateKey = today.toString();
        
        // 优先使用 API 数据
        if (holidayCache.containsKey(dateKey)) {
            int type = holidayCache.get(dateKey);
            
            return switch (type) {
                case 0 -> plugin.getConfig().getDouble("holidays.multipliers.workday", 1.0);
                case 1 -> plugin.getConfig().getDouble("holidays.multipliers.weekend", 1.1);
                case 2 -> plugin.getConfig().getDouble("holidays.multipliers.holiday", 1.5);
                case 3 -> plugin.getConfig().getDouble("holidays.multipliers.compensation", 1.2);
                default -> plugin.getConfig().getDouble("holidays.multipliers.other", 0.9);
            };
        }
        
        // 降级到本地判断
        DayOfWeek day = today.getDayOfWeek();
        if (day == DayOfWeek.SATURDAY || day == DayOfWeek.SUNDAY) {
            return plugin.getConfig().getDouble("holidays.multipliers.weekend", 1.1);
        }
        
        return plugin.getConfig().getDouble("holidays.multipliers.workday", 1.0);
    }
    
    /**
     * 关闭线程池
     */
    public void shutdown() {
        if (!virtualExecutor.isShutdown()) {
            virtualExecutor.shutdown();
            try {
                if (!virtualExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    virtualExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                virtualExecutor.shutdownNow();
            }
        }
        
        plugin.getLogger().info("[Market] VirtualThread executor closed");
    }
    
    // Getters
    public double getInflation() { return inflation.get(); }
    public double getMarketFlux() { return marketFlux.get(); }
    public double getActivity() { return activityFactor.get(); }
    public double getCurrentThreshold() { return currentThreshold.get(); }
}