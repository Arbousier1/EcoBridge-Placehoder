package top.ellan.ecobridge;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bukkit.Bukkit;
import org.bukkit.OfflinePlayer;
import org.bukkit.Statistic; // [修复] 正确的 Bukkit 统计枚举导入
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

public class MarketManager {

    private final EcoBridge plugin;
    
    // [优化] 使用 Java 21+ 虚拟线程执行器处理 IO 密集型任务
    private final ExecutorService virtualExecutor = Executors.newVirtualThreadPerTaskExecutor();

    // 核心数据 (线程安全引用)
    private final AtomicReference<Double> inflation = new AtomicReference<>(1.0);
    private final AtomicReference<Double> marketFlux = new AtomicReference<>(1.0);
    private final AtomicReference<Double> currentThreshold = new AtomicReference<>(100000.0);
    private final AtomicReference<Double> activityFactor = new AtomicReference<>(1.0);
    
    // 节假日缓存
    private final ConcurrentHashMap<String, Integer> holidayCache = new ConcurrentHashMap<>();
    private long lastHolidayUpdate = 0;
    
    private final HttpClient httpClient;
    private final ObjectMapper jsonMapper;
    private Currency currency;

    // 配置常量
    private static final double MIN_THRESH = 100000.0;
    private static final double INF_WEIGHT = 0.3;
    private static final double PERCENTILE = 0.15; // Top 15%
    private static final String HOLIDAY_API_URL = "https://holiday.ailcc.com/api/holiday/allyear/";

    public MarketManager(EcoBridge plugin) {
        this.plugin = plugin;
        
        // 初始化 HTTP Client (Java 11+)
        this.httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .connectTimeout(Duration.ofSeconds(10))
                .executor(virtualExecutor) // 让 HttpClient 内部也使用虚拟线程
                .build();
        this.jsonMapper = new ObjectMapper();
        
        setupCurrency();
    }
    
    private void setupCurrency() {
        if (Bukkit.getPluginManager().getPlugin("CoinsEngine") != null) {
            String currencyId = plugin.getConfig().getString("economy-settings.currency-id", "ellan_gold");
            this.currency = CoinsEngineAPI.getCurrency(currencyId);
            if (this.currency == null) {
                plugin.getLogger().warning("Currency '" + currencyId + "' not found in CoinsEngine!");
            }
        }
    }

    // =========================================================================
    // 核心计算逻辑 (线程安全增强版)
    // =========================================================================

    /**
     * 计算玩家的个人价格系数 (富人税 + 老玩家福利)
     * [安全] 可在任何线程调用，内部会自动处理 Bukkit API 的线程约束
     */
    public double calculatePersonalFactor(Player player) {
        if (player.isOp()) return 1.0;
        
        // 1. 获取配置
        double minFactor = plugin.getConfig().getDouble("personal-factor.min-factor", 0.5);
        double maxTax = plugin.getConfig().getDouble("personal-factor.rich.max-tax", 0.1);
        double richCap = maxTax * 2;
        double richMult = maxTax / 10.0; 
        
        double currentFactor = 1.0;
        
        // 2. 富人税计算 (使用 Log10 抑制巨额财富)
        if (currency != null) {
            // CoinsEngine 内部处理了数据库连接，通常可以直接调用
            double balance = CoinsEngineAPI.getBalance(player.getUniqueId(), currency);
            double threshold = currentThreshold.get();
            
            if (balance > threshold) {
                double excess = balance - threshold;
                // Java Math.log10 = 以10为底的对数
                double tax = Math.log10(excess + 10) * richMult;
                currentFactor += Math.min(tax, richCap);
            }
        }
        
        // 3. 老玩家福利 (PlayTime 统计)
        // [修复] 使用 getSafeStatistic 替代直接调用，防止异步访问 Bukkit API 报错
        int ticksPlayed = getSafeStatistic(player, Statistic.PLAY_ONE_MINUTE);
        
        if (ticksPlayed > 0) {
            double vetMax = plugin.getConfig().getDouble("personal-factor.veteran.max-discount", 0.05);
            int vetHours = plugin.getConfig().getInt("personal-factor.veteran.threshold-hours", 100);
            long targetTicks = vetHours * 72000L; // 1 hour = 3600s = 72000 ticks
            
            // 线性插值计算折扣
            double discount = (double) ticksPlayed / targetTicks * vetMax;
            currentFactor -= Math.min(discount, vetMax);
        }
        
        return Math.max(minFactor, currentFactor);
    }

    /**
     * [关键辅助方法] 线程安全的统计数据读取器
     * 如果当前是主线程：直接读取
     * 如果当前是虚拟/异步线程：调度到主线程并阻塞等待结果 (利用虚拟线程特性，零性能损耗)
     */
    private int getSafeStatistic(Player player, Statistic statistic) {
        if (Bukkit.isPrimaryThread()) {
            return player.getStatistic(statistic);
        } else {
            try {
                // callSyncMethod 返回 Future，.get() 会阻塞当前虚拟线程直到主线程执行完毕
                // 对于虚拟线程来说，这种阻塞仅仅是挂起（Unmount），不会占用操作系统线程
                return Bukkit.getScheduler().callSyncMethod(plugin, () -> player.getStatistic(statistic)).get();
            } catch (Exception e) {
                // 如果玩家下线或任务取消，返回 0 避免影响计算
                return 0;
            }
        }
    }

    /**
     * 更新全服经济指标 (通胀率 & 财富门槛)
     * [优化] 使用虚拟线程运行，防止大量数据库查询阻塞服务器通用线程池
     */
    public void updateEconomyMetrics() {
        if (currency == null) return;
        
        virtualExecutor.submit(() -> {
            try {
                // getOfflinePlayers 返回的是快照数组，在异步线程遍历是安全的
                OfflinePlayer[] allPlayers = Bukkit.getOfflinePlayers();
                
                // [优化] 使用 ArrayList 预估容量，减少扩容开销
                List<Double> balances = new ArrayList<>(Math.min(allPlayers.length, 5000));

                for (OfflinePlayer p : allPlayers) {
                    if (p.isOp()) continue; 
                    
                    // CoinsEngineAPI 可能触发数据库 IO
                    double bal = CoinsEngineAPI.getBalance(p.getUniqueId(), currency);
                    if (bal > 0) {
                        balances.add(bal);
                    }
                }

                if (balances.isEmpty()) return;

                // 3. 计算统计数据 (排序 + 分位数)
                Collections.sort(balances);
                
                int size = balances.size();
                int index = (int) Math.floor(size * (1.0 - PERCENTILE));
                index = Math.max(0, Math.min(size - 1, index));
                
                double threshold = Math.max(balances.get(index), MIN_THRESH);
                
                // 4. 更新原子状态
                currentThreshold.set(threshold);

                // 计算通胀率: Log(当前门槛 / 基础门槛) * 权重
                double ratio = Math.max(1.0, threshold / MIN_THRESH);
                double newInflation = 1.0 + (Math.log(ratio) * INF_WEIGHT);
                
                inflation.set(newInflation);
                
                plugin.getLogger().info(String.format("Economy Metrics Updated (VirtualThread): Users=%d, Thresh=%.0f, Inflation=%.4f", size, threshold, newInflation));

            } catch (Exception e) {
                plugin.getLogger().warning("Failed to update economy metrics: " + e.getMessage());
            }
        });
    }

    /**
     * 更新市场波动值 (Market Flux)
     */
    public void updateMarketFlux() {
        // 纯计算任务
        long currentHour = System.currentTimeMillis() / 3600000L; 
        Random rng = new Random(currentHour); 

        double range = plugin.getConfig().getDouble("market-flux.normal-range", 0.05);
        if (rng.nextDouble() < plugin.getConfig().getDouble("market-flux.event-chance", 0.1)) {
            range = plugin.getConfig().getDouble("market-flux.event-range", 0.3); 
        }

        double rawFlux = 1.0 + ((rng.nextDouble() * 2.0 - 1.0) * range);
        double finalFlux = rawFlux * inflation.get();
        
        // 保留3位小数
        finalFlux = Math.floor(finalFlux * 1000) / 1000.0;
        
        marketFlux.set(finalFlux);
    }

    /**
     * 更新活跃度因子 (TPS & 在线人数)
     * [注意] 必须在主线程 (Sync) 运行
     */
    public void updateActivityFactor() {
        Bukkit.getScheduler().runTask(plugin, () -> {
            int online = Bukkit.getOnlinePlayers().size();
            
            // 获取最近 1 分钟的 TPS
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
            
            double tpsLimit = plugin.getConfig().getDouble("activity.tps-limit", 18.0);
            if (tps < tpsLimit) {
                double weight = plugin.getConfig().getDouble("activity.tps-weight", 0.05);
                factor += (20.0 - Math.max(5.0, tps)) * weight;
            }
            activityFactor.set(factor);
        });
    }

    /**
     * 更新节假日缓存 (HTTP 请求)
     */
    public void updateHolidayCache() {
        if (System.currentTimeMillis() - lastHolidayUpdate < 86400000) return; // 24h CD

        int year = LocalDate.now().getYear();
        String url = HOLIDAY_API_URL + "?year=" + year;

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("User-Agent", "EcoBridge/Java25")
                .GET()
                .build();

        // 异步发送请求
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
                                plugin.getLogger().info("Holiday cache updated via VirtualThread (" + year + ")");
                            }
                        }
                    } catch (Exception e) {
                        plugin.getLogger().warning("Failed to parse holiday JSON: " + e.getMessage());
                    }
                })
                .exceptionally(ex -> {
                    plugin.getLogger().warning("Holiday API request failed: " + ex.getMessage());
                    return null;
                });
    }

    public double getHolidayFactor() {
        LocalDate today = LocalDate.now();
        String dateKey = today.toString(); // yyyy-MM-dd

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

        DayOfWeek day = today.getDayOfWeek();
        if (day == DayOfWeek.SATURDAY || day == DayOfWeek.SUNDAY) {
            return plugin.getConfig().getDouble("holidays.multipliers.weekend", 1.1);
        }

        return plugin.getConfig().getDouble("holidays.multipliers.workday", 1.0);
    }

    // 关闭线程池
    public void shutdown() {
        if (!virtualExecutor.isShutdown()) {
            virtualExecutor.shutdownNow();
        }
    }

    public double getInflation() { return inflation.get(); }
    public double getMarketFlux() { return marketFlux.get(); }
    public double getActivity() { return activityFactor.get(); }
    public double getCurrentThreshold() { return currentThreshold.get(); }
}