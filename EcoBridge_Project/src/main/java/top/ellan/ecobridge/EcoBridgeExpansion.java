package top.ellan.ecobridge;

import jdk.incubator.vector.DoubleVector;
import me.clip.placeholderapi.expansion.PlaceholderExpansion;
import org.bukkit.OfflinePlayer;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ConcurrentHashMap;

/**
 * PAPI 变量扩展 - 最终优化版
 * 集成了 PID 税率变量、SIMD 监控与高效缓存
 */
public class EcoBridgeExpansion extends PlaceholderExpansion {

    private final EcoBridge plugin;

    // ==================== 缓存系统 ====================
    // Key: Player UUID String
    private final ConcurrentHashMap<String, PersonalFactorCache> factorCache = new ConcurrentHashMap<>();
    private static final long CACHE_TTL_MS = 2000; // 2秒缓存，完美适配 Scoreboard 刷新率
    private long lastCacheCleanup = System.currentTimeMillis();

    // Java 16+ Record，内存开销极低
    private record PersonalFactorCache(double value, long timestamp) {
        boolean isExpired() {
            return System.currentTimeMillis() - timestamp > CACHE_TTL_MS;
        }
    }

    public EcoBridgeExpansion(EcoBridge plugin) {
        this.plugin = plugin;
    }

    @Override
    public @NotNull String getIdentifier() {
        return "eco";
    }

    @Override
    public @NotNull String getAuthor() {
        return "Ellan";
    }

    @Override
    public @NotNull String getVersion() {
        return "2.0.0";
    }

    @Override
    public boolean persist() {
        return true; 
    }

    @Override
    public String onRequest(OfflinePlayer player, @NotNull String params) {
        // 1. 基础状态检查 (Fail-Fast)
        if (!plugin.isEnabled()) return "Loading...";
        if (!plugin.isFullyInitialized()) return "Warming Up...";

        MarketManager market = plugin.getMarketManager();
        if (market == null) return "Error";

        // 2. 惰性清理缓存 (避免高频迭代 Map)
        cleanupCache();

        // 3. 统一转小写处理 (只做一次)
        String lowerParams = params.toLowerCase();

        // ==================== A. 全局市场指标 (直接读取 Volatile) ====================
        switch (lowerParams) {
            case "inflation" -> { return formatDecimal(market.getInflation(), 4); }
            case "inflation_raw" -> { return String.valueOf(market.getInflation()); }
            case "inflation_percent" -> { return formatPercent(market.getInflation() - 1.0); }

            case "flux" -> { return formatDecimal(market.getMarketFlux(), 4); }
            case "flux_raw" -> { return String.valueOf(market.getMarketFlux()); }

            case "activity" -> { return formatDecimal(market.getActivity(), 4); }
            case "activity_raw" -> { return String.valueOf(market.getActivity()); }

            case "threshold" -> { return formatInteger(market.getCurrentThreshold()); }
            case "threshold_raw" -> { return String.valueOf(market.getCurrentThreshold()); }
            case "threshold_k" -> { return formatThousands(market.getCurrentThreshold()); }

            case "holiday" -> { return formatDecimal(market.getHolidayFactor(), 2); }
            case "holiday_raw" -> { return String.valueOf(market.getHolidayFactor()); }
            case "holiday_percent" -> { return formatPercent(market.getHolidayFactor() - 1.0); }
            
            // [新功能] PID 建议税率
            case "tax_rate" -> { return formatPercent(market.getSuggestedTaxRate()); }
            case "tax_rate_raw" -> { return String.valueOf(market.getSuggestedTaxRate()); }
        }

        // ==================== B. 玩家个人指标 (带缓存) ====================
        if (lowerParams.startsWith("personal")) {
            double factor = getOrCalculatePersonal(player, market);
            if (lowerParams.equals("personal_raw")) return String.valueOf(factor);
            return formatDecimal(factor, 4);
        }

        if (lowerParams.startsWith("final")) {
            double factor = getOrCalculateFinal(player, market);
            
            if (lowerParams.equals("final_raw")) return String.valueOf(factor);
            if (lowerParams.equals("final_percent")) return formatPercent(factor - 1.0);
            return formatDecimal(factor, 4);
        }

        // ==================== C. 性能监控指标 ====================
        if (lowerParams.startsWith("perf_")) {
            return switch (lowerParams) {
                case "perf_simd_active" -> String.valueOf(plugin.getPerformanceMonitor() != null);
                // 显示具体的 SIMD 向量位宽 (如 256-bit)，用于确认 AVX2 是否生效
                case "perf_simd_width" -> (DoubleVector.SPECIES_PREFERRED.length() * 64) + "-bit";
                case "perf_memory_used" -> {
                    long usedMB = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1048576;
                    yield usedMB + "MB";
                }
                case "perf_memory_percent" -> {
                    long total = Runtime.getRuntime().totalMemory();
                    long free = Runtime.getRuntime().freeMemory();
                    yield formatPercent((double) (total - free) / Runtime.getRuntime().maxMemory());
                }
                default -> null;
            };
        }

        // ==================== D. PID 动态查询 ====================
        // 格式: %eco_pid_<shopId_productId>% 或 %eco_pid_raw_<...>%
        if (lowerParams.startsWith("pid_")) {
            PidController pid = plugin.getPidController();
            if (pid == null) return "N/A";

            try {
                boolean isRaw = lowerParams.startsWith("pid_raw_");
                // 智能截取 ID
                String itemId = isRaw ? params.substring(8) : params.substring(4); 

                // 调用 PID 控制器的高速查找
                double lambda = pid.getLambdaByString(itemId);

                return isRaw ? String.valueOf(lambda) : formatDecimal(lambda, 5);
            } catch (Exception e) {
                return "Invalid ID";
            }
        }

        return null;
    }

    // ==================== 缓存逻辑 ====================

    private void cleanupCache() {
        long now = System.currentTimeMillis();
        // 每 5 秒最多检查一次，减少开销
        if (now - lastCacheCleanup > 5000) {
            factorCache.entrySet().removeIf(entry -> entry.getValue().isExpired());
            lastCacheCleanup = now;
        }
    }

    private double getOrCalculatePersonal(OfflinePlayer player, MarketManager market) {
        if (player == null) return 1.0;
        
        // 如果玩家离线，无法获取统计数据，直接返回 1.0 且不缓存
        if (!player.isOnline()) return 1.0;

        String uuid = player.getUniqueId().toString();
        PersonalFactorCache cached = factorCache.get(uuid);

        if (cached != null && !cached.isExpired()) {
            return cached.value;
        }

        // 计算并写入缓存
        double val = market.calculatePersonalFactor(player.getPlayer());
        factorCache.put(uuid, new PersonalFactorCache(val, System.currentTimeMillis()));
        return val;
    }

    private double getOrCalculateFinal(OfflinePlayer player, MarketManager market) {
        // Final = Personal * Inflation * Flux * Activity * Holiday
        // 只有 Personal 是针对玩家的，其他是全局 volatile 变量，读取极快
        double personal = getOrCalculatePersonal(player, market);
        
        return personal 
             * market.getInflation() 
             * market.getMarketFlux() 
             * market.getActivity() 
             * market.getHolidayFactor();
    }

    // ==================== 格式化工具 ====================

    private String formatDecimal(double value, int decimals) {
        return String.format("%." + decimals + "f", value);
    }

    private String formatInteger(double value) {
        return String.format("%,.0f", value);
    }

    private String formatThousands(double value) {
        if (value >= 1_000_000) {
            return String.format("%.1fM", value / 1_000_000);
        } else if (value >= 1_000) {
            return String.format("%.1fK", value / 1_000);
        } else {
            return String.format("%.0f", value);
        }
    }

    private String formatPercent(double ratio) {
        // 例如 0.05 -> +5.0%
        return String.format("%+.1f%%", ratio * 100);
    }
}