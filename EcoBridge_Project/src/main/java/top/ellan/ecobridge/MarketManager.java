package top.ellan.ecobridge;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bukkit.Bukkit;
import org.bukkit.Statistic;
import org.bukkit.entity.Player;
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

/**
 * 市场管理器 - 极致性能版
 * 特性：
 * 1. 无锁设计 (Volatile/VarHandle)
 * 2. 读写分离 (Main-Thread Read, Virtual-Thread Compute)
 * 3. 零阻塞缓存
 */
public class MarketManager {

    private final EcoBridge plugin;
    
    // I/O 执行器 (HTTP, 重型计算)
    private final ExecutorService ioExecutor = Executors.newVirtualThreadPerTaskExecutor();

    // ==================== 1. 高频热数据 (Volatile 极速读取) ====================
    // 相比 AtomicReference，基本类型 volatile 减少了引用跳转和 GC
    private volatile double inflation = 1.0;
    private volatile double marketFlux = 1.0;
    private volatile double currentThreshold = 100000.0;
    private volatile double activityFactor = 1.0;
    
    // PID 计算结果 (由主循环更新)
    private volatile double suggestedTaxRate = 0.05;

    // ==================== 2. 缓存系统 ====================
    // 假日缓存 (日期 -> 类型)
    private final ConcurrentHashMap<String, Integer> holidayCache = new ConcurrentHashMap<>();
    private volatile long lastHolidayUpdate = 0;

    // 玩家统计缓存 (UUID -> <Value, Timestamp>)
    // 使用 ConcurrentHashMap 保证并发安全，虽然写入是在主线程，但读取可能在任意线程
    private final ConcurrentHashMap<UUID, StatEntry> statCache = new ConcurrentHashMap<>(1024);
    private record StatEntry(int value, long timestamp) {}

    // ==================== 3. 外部组件 ====================
    private final HttpClient httpClient;
    private final ObjectMapper jsonMapper = new ObjectMapper();
    
    // CoinsEngine 引用
    private CoinsEnginePlugin coinsEnginePlugin;
    private Currency currency;

    // 配置常量 (加载后缓存，避免 Map 查找)
    private final double minThreshold;
    private final double inflationWeight;
    private final double percentile;
    private static final String HOLIDAY_API_URL = "https://api.apihubs.cn/holiday/get";

    public MarketManager(EcoBridge plugin) {
        this.plugin = plugin;
        
        // 预加载配置
        this.minThreshold = 100000.0; // 可以从 config 读取
        this.inflationWeight = 0.3;
        this.percentile = 0.15;

        this.httpClient = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_2)
            .connectTimeout(Duration.ofSeconds(10))
            .executor(ioExecutor)
            .build();

        setupCoinsEngine();
    }

    // ==================== 核心集成方法 (被 EcoBridge 调用) ====================

    /**
     * 应用 PID 计算结果 (运行在 Main Thread)
     * 来自 EcoBridge 主循环 Phase 3
     */
    public void applyCalculationResult(EcoBridge.CalculationResult result) {
        // 直接更新 volatile 变量，对所有线程立即可见
        // 这里我们可以引入一个平滑过渡 (Lerp) 避免突变，但为了性能直接赋值
        this.inflation = Math.max(0.1, result.globalInflationRate()); // 防止通缩过头
        this.suggestedTaxRate = Math.max(0.0, Math.min(0.5, result.suggestedTaxRate()));
    }

    // ==================== 异步更新任务 ====================

    /**
     * 更新经济指标 (Thread-Safe & Optimized)
     * 逻辑：Async (Start) -> Sync (Fetch Data) -> Async (Compute)
     */
    public void updateEconomyMetrics() {
        if (currency == null || coinsEnginePlugin == null) return;

        // 1. 调度到主线程获取数据 (Paper API 安全原则)
        Bukkit.getScheduler().runTask(plugin, () -> {
            // 检查 TopManager
            Optional<TopManager> topManagerOpt = coinsEnginePlugin.getTopManager();
            if (topManagerOpt.isEmpty()) return;

            // 获取原始列表 (这通常涉及内存复制，但在主线程是安全的)
            List<TopEntry> entries = topManagerOpt.get().getTopEntries(currency);
            if (entries.isEmpty()) return;

            // 2. 将数据处理移交回虚拟线程 (避免阻塞主线程进行数学计算)
            // 我们需要复制一份数据或者确保 TopEntry 是不可变的/线程安全的 POJO
            List<TopEntry> snapshot = new ArrayList<>(entries); 
            
            ioExecutor.submit(() -> computeEconomyMetricsAsync(snapshot));
        });
    }

    private void computeEconomyMetricsAsync(List<TopEntry> entries) {
        try {
            int totalPlayers = entries.size();
            
            // 计算分位点
            int thresholdIndex = (int) Math.floor(totalPlayers * percentile);
            thresholdIndex = Math.max(0, Math.min(totalPlayers - 1, thresholdIndex));

            TopEntry thresholdEntry = entries.get(thresholdIndex);
            double newThreshold = Math.max(thresholdEntry.getBalance(), minThreshold);

            // 更新阈值
            this.currentThreshold = newThreshold;

            // 这里只是辅助计算基准通胀，真正的通胀由 PID 控制
            // 但我们可以保留这个作为 PID 的参考或基准值
            double ratio = Math.max(1.0, newThreshold / minThreshold);
            double baseInflation = 1.0 + (Math.log(ratio) * inflationWeight);
            
            // 记录日志 (可选，减少 I/O)
            if (plugin.getConfig().getBoolean("debug", false)) {
                plugin.getLogger().info(String.format(
                    "[Market] Async Metrics: Players=%d, Thresh=%.0f, BaseInf=%.4f",
                    totalPlayers, newThreshold, baseInflation
                ));
            }
        } catch (Exception e) {
            plugin.getLogger().warning("[Market] Async calculation failed: " + e.getMessage());
        }
    }

    /**
     * 更新活跃度因子
     * 逻辑：Sync (Capture) -> Async (Compute)
     */
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
                this.activityFactor = factor;
            });
        });
    }

    public void updateMarketFlux() {
        long currentHour = System.currentTimeMillis() / 3600000L;
        // 使用简单的伪随机，避免 Random 对象创建开销
        long seed = currentHour * 31 + 17;
        Random rng = new Random(seed);

        double range = plugin.getConfig().getDouble("market-flux.normal-range", 0.05);
        if (rng.nextDouble() < plugin.getConfig().getDouble("market-flux.event-chance", 0.1)) {
            range = plugin.getConfig().getDouble("market-flux.event-range", 0.3);
        }

        double rawFlux = 1.0 + ((rng.nextDouble() * 2.0 - 1.0) * range);
        // 保留 4 位小数
        this.marketFlux = Math.floor(rawFlux * this.inflation * 10000) / 10000.0;
    }

    // ==================== 业务逻辑 (热路径) ====================

    /**
     * 计算个人系数 (Hot Path)
     * 该方法通常在主线程 (玩家交互时) 调用
     */
    public double calculatePersonalFactor(Player player) {
        if (player.isOp()) return 1.0;

        double currentFactor = 1.0;

        // 1. 富人税 (CoinsEngine API 在主线程通常是安全的，且可能有缓存)
        if (currency != null) {
            double balance = CoinsEngineAPI.getBalance(player.getUniqueId(), currency);
            double threshold = this.currentThreshold; // Volatile read

            if (balance > threshold) {
                double maxTax = plugin.getConfig().getDouble("personal-factor.rich.max-tax", 0.1);
                double excess = balance - threshold;
                // 使用 fast math 或查表可以更快，但 Math.log10 纳秒级也足够
                double tax = Math.log10(excess + 10) * (maxTax / 10.0);
                currentFactor += Math.min(tax, maxTax * 2);
            }
        }

        // 2. 老玩家折扣 (使用优化的缓存)
        int ticksPlayed = getCachedStatistic(player, Statistic.PLAY_ONE_MINUTE);
        if (ticksPlayed > 0) {
            double vetMax = plugin.getConfig().getDouble("personal-factor.veteran.max-discount", 0.05);
            int vetHours = plugin.getConfig().getInt("personal-factor.veteran.threshold-hours", 100);
            
            // 避免除零
            long denominator = vetHours * 72000L;
            if (denominator > 0) {
                double discount = (double) ticksPlayed / denominator * vetMax;
                currentFactor -= Math.min(discount, vetMax);
            }
        }

        double minFactor = plugin.getConfig().getDouble("personal-factor.min-factor", 0.5);
        return Math.max(minFactor, currentFactor);
    }

    // ==================== 辅助与缓存 ====================

    /**
     * 优化的统计数据获取
     * 策略：如果缓存有效，直接返回。如果缓存过期，返回旧值并异步调度更新。
     * 绝不阻塞当前线程去调用 player.getStatistic()
     */
    private int getCachedStatistic(Player player, Statistic stat) {
        UUID uid = player.getUniqueId();
        StatEntry entry = statCache.get(uid);
        long now = System.currentTimeMillis();

        // 缓存命中且未过期 (5分钟)
        if (entry != null && (now - entry.timestamp() < 300_000)) {
            return entry.value();
        }

        // 缓存过期或不存在，需要更新
        // 只有在主线程才能安全调用 getStatistic
        if (Bukkit.isPrimaryThread()) {
            int val = player.getStatistic(stat);
            statCache.put(uid, new StatEntry(val, now));
            return val;
        } else {
            // 如果在异步线程调用，且缓存不存在，只能返回 0 (无法安全获取)
            // 如果缓存存在但过期，返回旧值 (Soft Expiry)
            
            // 调度一个更新任务给下一次使用
            Bukkit.getScheduler().runTask(plugin, () -> {
                if (player.isOnline()) {
                    statCache.put(uid, new StatEntry(player.getStatistic(stat), System.currentTimeMillis()));
                }
            });
            
            return entry != null ? entry.value() : 0;
        }
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
        // LocalDate.now() 有微小开销，可以缓存到 System.currentTimeMillis() 转换
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

    // ==================== 初始化与清理 ====================
    private void setupCoinsEngine() {
        if (Bukkit.getPluginManager().isPluginEnabled("CoinsEngine") && CoinsEngineAPI.isLoaded()) {
            this.coinsEnginePlugin = CoinsEngineAPI.plugin();
            String currencyId = plugin.getConfig().getString("economy-settings.currency-id", "money");
            this.currency = CoinsEngineAPI.getCurrency(currencyId);

            if (this.currency != null) {
                plugin.getLogger().info("[Market] Hooked into CoinsEngine. Currency: " + currencyId);
            } else {
                plugin.getLogger().warning("[Market] Currency '" + currencyId + "' not found!");
            }
        }
    }

    public void shutdown() {
        ioExecutor.shutdownNow();
    }

    // ==================== Getters (Volatile Read) ====================
    public double getInflation() { return inflation; }
    public double getMarketFlux() { return marketFlux; }
    public double getActivity() { return activityFactor; }
    public double getCurrentThreshold() { return currentThreshold; }
    public double getSuggestedTaxRate() { return suggestedTaxRate; }
}