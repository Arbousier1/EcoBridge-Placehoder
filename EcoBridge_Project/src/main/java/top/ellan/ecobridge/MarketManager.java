package top.ellan.ecobridge;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bukkit.Bukkit;
import org.bukkit.OfflinePlayer;
import su.nightexpress.coinsengine.api.CoinsEngineAPI;
import su.nightexpress.coinsengine.api.currency.Currency;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
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
    // 相比 Bukkit 的 runTaskAsynchronously (使用平台线程)，虚拟线程在处理 HTTP/SQL 等待时几乎零开销
    private final ExecutorService virtualExecutor = Executors.newVirtualThreadPerTaskExecutor();

    // 核心数据 (线程安全引用)
    private final AtomicReference<Double> inflation = new AtomicReference<>(1.0);
    private final AtomicReference<Double> marketFlux = new AtomicReference<>(1.0);
    private final AtomicReference<Double> currentThreshold = new AtomicReference<>(100000.0);
    private final AtomicReference<Double> activityFactor = new AtomicReference<>(1.0);
    
    // 节假日缓存
    private final ConcurrentHashMap<String, Integer> holidayCache = new ConcurrentHashMap<>();
    private final HttpClient httpClient;
    private final ObjectMapper jsonMapper;

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
    }

    /**
     * 更新全服经济指标 (通胀率 & 财富门槛)
     * [优化] 使用虚拟线程运行，防止大量数据库查询阻塞服务器通用线程池
     */
    public void updateEconomyMetrics() {
        if (!Bukkit.getPluginManager().isPluginEnabled("CoinsEngine")) return;
        
        virtualExecutor.submit(() -> {
            try {
                // 1. 获取配置与货币
                // 注意：config读取通常不耗时，但建议在主线程预加载。这里为了逻辑完整保留读取。
                String currencyId = plugin.getConfig().getString("economy-settings.currency-id", "ellan_gold");
                Currency currency = CoinsEngineAPI.getCurrency(currencyId);
                
                if (currency == null) {
                    plugin.getLogger().warning("Currency '" + currencyId + "' not found in CoinsEngine!");
                    return;
                }

                // 2. 获取玩家列表 (注意：getOfflinePlayers 可能会返回大量数据)
                // Bukkit API 的 getOfflinePlayers 返回的是数组拷贝，线程安全，但可能较慢
                OfflinePlayer[] allPlayers = Bukkit.getOfflinePlayers();
                
                // [优化] 使用 ArrayList 预估容量，减少扩容开销
                List<Double> balances = new ArrayList<>(Math.min(allPlayers.length, 10000));

                for (OfflinePlayer p : allPlayers) {
                    if (p.isOp()) continue; // 排除管理员
                    
                    // [修复] getBalance 必须传入 UUID
                    // CoinsEngine 的 getBalance 可能会触发数据库查询，这正是虚拟线程发挥作用的地方
                    double bal = CoinsEngineAPI.getBalance(p.getUniqueId(), currency);
                    
                    if (bal > 0) {
                        balances.add(bal);
                    }
                }

                if (balances.isEmpty()) return;

                // 3. 计算统计数据 (排序 + 分位数)
                // 对于基本类型 Double，Java 的 TimSort 已经非常高效
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
     * 逻辑：确定性随机 (基于小时种子)
     */
    public void updateMarketFlux() {
        // 纯计算任务，无需虚拟线程，直接在调用线程执行即可
        long currentHour = System.currentTimeMillis() / 3600000L; 
        Random rng = new Random(currentHour); 

        double range = 0.05; 
        if (rng.nextDouble() < 0.1) {
            range = 0.3; 
        }

        double rawFlux = 1.0 + ((rng.nextDouble() * 2.0 - 1.0) * range);
        double finalFlux = rawFlux * inflation.get();
        
        marketFlux.set(finalFlux);
    }

    /**
     * 更新活跃度因子 (TPS & 在线人数)
     * [注意] 必须在主线程 (Sync) 运行，因为 Bukkit.getTPS() 和 getOnlinePlayers() 非线程安全
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
            } catch (Throwable ignored) {
                // 兼容性兜底 (某些非 Paper 服务端可能没有 getTPS)
            }

            double factor = 1.0 + (online * 0.001); 
            
            if (tps < 18.0) {
                // TPS 越低，价格越高 (抑制交易)
                factor += (20.0 - tps) * 0.05;
            }
            activityFactor.set(factor);
        });
    }

    /**
     * 更新节假日缓存 (HTTP 请求)
     * [优化] 使用 HttpClient 异步 API + 虚拟线程回调
     */
    public void updateHolidayCache() {
        int year = LocalDate.now().getYear();
        String url = HOLIDAY_API_URL + "?year=" + year;

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("User-Agent", "EcoBridge-JavaClient/1.0")
                .GET()
                .build();

        // 异步发送请求，Future 回调也会在 Executor (虚拟线程) 中执行
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
        String dateKey = String.format("%d-%d-%d", today.getYear(), today.getMonthValue(), today.getDayOfMonth());

        if (holidayCache.containsKey(dateKey)) {
            int type = holidayCache.get(dateKey);
            return switch (type) {
                case 0 -> 1.0; 
                case 1 -> 1.1; 
                case 2 -> 1.5; 
                case 3 -> 1.2; 
                default -> 1.0;
            };
        }

        int dayOfWeek = today.getDayOfWeek().getValue(); 
        if (dayOfWeek == 6 || dayOfWeek == 7) {
            return 1.1;
        }

        return 1.0;
    }

    // 关闭线程池 (在插件 disable 时调用)
    public void shutdown() {
        if (!virtualExecutor.isShutdown()) {
            virtualExecutor.shutdown();
        }
    }

    public double getInflation() { return inflation.get(); }
    public double getMarketFlux() { return marketFlux.get(); }
    public double getActivity() { return activityFactor.get(); }
    public double getCurrentThreshold() { return currentThreshold.get(); }
}