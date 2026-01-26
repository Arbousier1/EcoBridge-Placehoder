package top.ellan.ecobridge;

import me.clip.placeholderapi.expansion.PlaceholderExpansion;
import org.bukkit.OfflinePlayer;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ConcurrentHashMap;

public class EcoBridgeExpansion extends PlaceholderExpansion {
    
    private final EcoBridge plugin;
    
    // 玩家个人系数缓存
    private final ConcurrentHashMap<String, CachedValue> personalFactorCache = new ConcurrentHashMap<>();
    private static final long CACHE_TTL_MS = 1000;
    private long lastCacheCleanup = System.currentTimeMillis();
    
    private static class CachedValue {
        final double value;
        final long timestamp;
        
        CachedValue(double value) {
            this.value = value;
            this.timestamp = System.currentTimeMillis();
        }
        
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
        if (!plugin.isEnabled()) return "Loading...";
        
        MarketManager market = plugin.getMarketManager();
        if (market == null) return "Error";
        
        cleanupCache();

        return switch (params.toLowerCase()) {
            // ==================== 全局市场指标 ====================
            case "inflation" -> formatDecimal(market.getInflation(), 4);
            case "inflation_raw" -> String.valueOf(market.getInflation());
            case "inflation_percent" -> formatPercent(market.getInflation() - 1.0);
            
            case "flux" -> formatDecimal(market.getMarketFlux(), 4);
            case "flux_raw" -> String.valueOf(market.getMarketFlux());
            
            case "activity" -> formatDecimal(market.getActivity(), 4);
            case "activity_raw" -> String.valueOf(market.getActivity());
            
            case "threshold" -> formatInteger(market.getCurrentThreshold());
            case "threshold_raw" -> String.valueOf(market.getCurrentThreshold());
            case "threshold_k" -> formatThousands(market.getCurrentThreshold());
            
            case "holiday" -> formatDecimal(market.getHolidayFactor(), 2);
            case "holiday_raw" -> String.valueOf(market.getHolidayFactor());
            case "holiday_percent" -> formatPercent(market.getHolidayFactor() - 1.0);
            
            // ==================== 玩家个人指标 ====================
            case "personal" -> getPersonalFactor(player, market, 4);
            case "personal_raw" -> getPersonalFactor(player, market, -1);
            
            case "final", "final_all" -> getFinalFactor(player, market, 4);
            case "final_raw", "final_all_raw" -> getFinalFactor(player, market, -1);
            case "final_percent" -> {
                double factor = calculateFinalFactor(player, market);
                yield formatPercent(factor - 1.0);
            }
            
            // ==================== 性能监控指标 ====================
            case "perf_simd_active" -> String.valueOf(plugin.getPerformanceMonitor() != null); 
            
            case "perf_memory_used" -> {
                Runtime rt = Runtime.getRuntime();
                long usedMB = (rt.totalMemory() - rt.freeMemory()) / 1048576;
                yield usedMB + "MB";
            }
            case "perf_memory_percent" -> {
                Runtime rt = Runtime.getRuntime();
                double percent = 100.0 * (rt.totalMemory() - rt.freeMemory()) / rt.maxMemory();
                yield formatPercent(percent / 100.0);
            }
            
            // ==================== PID 状态查询 ====================
            default -> {
                PidController pid = plugin.getPidController();
                if (pid == null) yield "N/A";

                // PID Lambda: %eco_pid_<shopId_productId>%
                if (params.startsWith("pid_")) {
                    String itemId = params.substring(4);
                    // [Fix] 使用新方法，不再有未使用的变量警告
                    double lambda = pid.getLambdaByString(itemId);
                    yield formatDecimal(lambda, 5);
                }
                
                // PID Raw Lambda
                if (params.startsWith("pid_raw_")) {
                    String itemId = params.substring(8);
                    double lambda = pid.getLambdaByString(itemId);
                    yield String.valueOf(lambda);
                }
                
                yield null;
            }
        };
    }
    
    // ==================== 辅助方法 ====================
    
    private void cleanupCache() {
        long now = System.currentTimeMillis();
        if (now - lastCacheCleanup > 5000) {
            personalFactorCache.entrySet().removeIf(entry -> entry.getValue().isExpired());
            lastCacheCleanup = now;
        }
    }

    private String getPersonalFactor(OfflinePlayer player, MarketManager market, int decimals) {
        if (player == null) return decimals < 0 ? "1.0" : "1.0000";
        
        String uuid = player.getUniqueId().toString();
        CachedValue cached = personalFactorCache.get(uuid);
        
        if (cached != null && !cached.isExpired()) {
            return decimals < 0 ? String.valueOf(cached.value) : 
                   String.format("%." + decimals + "f", cached.value);
        }
        
        if (player.isOnline()) {
            double factor = market.calculatePersonalFactor(player.getPlayer());
            personalFactorCache.put(uuid, new CachedValue(factor));
            return decimals < 0 ? String.valueOf(factor) : 
                   String.format("%." + decimals + "f", factor);
        }
        
        return decimals < 0 ? "1.0" : "1.0000";
    }
    
    private String getFinalFactor(OfflinePlayer player, MarketManager market, int decimals) {
        double factor = calculateFinalFactor(player, market);
        return decimals < 0 ? String.valueOf(factor) : 
               String.format("%." + decimals + "f", factor);
    }
    
    private double calculateFinalFactor(OfflinePlayer player, MarketManager market) {
        double personal = 1.0;
        
        if (player != null && player.isOnline()) {
            String uuid = player.getUniqueId().toString();
            CachedValue cached = personalFactorCache.get(uuid);
            
            if (cached != null && !cached.isExpired()) {
                personal = cached.value;
            } else {
                personal = market.calculatePersonalFactor(player.getPlayer());
                personalFactorCache.put(uuid, new CachedValue(personal));
            }
        }
        
        return personal 
             * market.getMarketFlux() 
             * market.getHolidayFactor() 
             * market.getActivity() 
             * market.getInflation();
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
        return String.format("%.1f%%", ratio * 100);
    }
}