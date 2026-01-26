package top.ellan.ecobridge;

import me.clip.placeholderapi.expansion.PlaceholderExpansion;
import org.bukkit.OfflinePlayer;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ConcurrentHashMap;

/**
 * EcoBridgeExpansion - PlaceholderAPI 集成
 * 
 * 优化点:
 * 1. 缓存机制 (减少重复计算)
 * 2. 空值安全检查
 * 3. 格式化选项 (%eco_inflation_raw% vs %eco_inflation%)
 * 4. 性能监控变量
 * 5. 统一错误处理
 */
public class EcoBridgeExpansion extends PlaceholderExpansion {
    
    private final EcoBridge plugin;
    
    // [优化] 玩家个人系数缓存 (1秒过期)
    private final ConcurrentHashMap<String, CachedValue> personalFactorCache = new ConcurrentHashMap<>();
    private static final long CACHE_TTL_MS = 1000;
    
    // 缓存值容器
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
        return plugin.getPluginMeta().getVersion();
    }
    
    @Override
    public boolean persist() {
        return true;
    }
    
    @Override
    public String onRequest(OfflinePlayer player, @NotNull String params) {
        // [安全检查] 插件未完全初始化时返回占位符
        if (!plugin.isFullyInitialized()) {
            return "§7初始化中...";
        }
        
        MarketManager market = plugin.getMarketManager();
        if (market == null) {
            return "§cERROR";
        }
        
        // [优化] 使用 switch 表达式提升性能
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
            case "perf_cache_size" -> String.valueOf(plugin.getPidController().getCacheSize());
            case "perf_dirty_size" -> String.valueOf(plugin.getPidController().getDirtyQueueSize());
            case "perf_cache_usage" -> {
                int size = plugin.getPidController().getCacheSize();
                int capacity = 128 * 1024; // 16 segments × 8192
                yield formatPercent((double) size / capacity);
            }
            
            // SIMD 统计
            case "perf_simd_ratio" -> {
                PerformanceMonitor monitor = plugin.getPerformanceMonitor();
                if (monitor != null) {
                    // 这里需要在 PerformanceMonitor 中添加 getter
                    yield "N/A";
                }
                yield "Disabled";
            }
            
            // 内存使用
            case "perf_memory_used" -> {
                Runtime rt = Runtime.getRuntime();
                long usedMB = (rt.totalMemory() - rt.freeMemory()) / 1048576;
                yield String.valueOf(usedMB) + "MB";
            }
            case "perf_memory_percent" -> {
                Runtime rt = Runtime.getRuntime();
                double percent = 100.0 * (rt.totalMemory() - rt.freeMemory()) / rt.maxMemory();
                yield formatPercent(percent / 100.0);
            }
            
            // ==================== PID 状态查询 ====================
            default -> {
                // PID 变量: %eco_pid_<shopId_productId>%
                if (params.startsWith("pid_")) {
                    String itemId = params.substring(4);
                    double lambda = plugin.getPidController().getCachedResult(itemId);
                    yield formatDecimal(lambda, 5);
                }
                
                // PID 原始值
                if (params.startsWith("pid_raw_")) {
                    String itemId = params.substring(8);
                    double lambda = plugin.getPidController().getCachedResult(itemId);
                    yield String.valueOf(lambda);
                }
                
                // PID 详细状态 (需要扩展 PidController API)
                if (params.startsWith("pid_integral_")) {
                    String itemId = params.substring(13);
                    var state = plugin.getPidController().inspectState(itemId);
                    yield state != null ? formatDecimal(state.integral(), 2) : "N/A";
                }
                
                if (params.startsWith("pid_error_")) {
                    String itemId = params.substring(10);
                    var state = plugin.getPidController().inspectState(itemId);
                    yield state != null ? formatDecimal(state.lastError(), 2) : "N/A";
                }
                
                // 未知参数
                yield null;
            }
        };
    }
    
    // ==================== 辅助方法 ====================
    
    /**
     * 获取玩家个人系数 (带缓存)
     */
    private String getPersonalFactor(OfflinePlayer player, MarketManager market, int decimals) {
        if (player == null || !player.isOnline()) {
            return decimals < 0 ? "1.0" : "1.0000";
        }
        
        String uuid = player.getUniqueId().toString();
        CachedValue cached = personalFactorCache.get(uuid);
        
        // 缓存命中且未过期
        if (cached != null && !cached.isExpired()) {
            return decimals < 0 ? String.valueOf(cached.value) : 
                   String.format("%." + decimals + "f", cached.value);
        }
        
        // 重新计算
        double factor = market.calculatePersonalFactor(player.getPlayer());
        personalFactorCache.put(uuid, new CachedValue(factor));
        
        // 定期清理过期缓存 (每100次调用)
        if (personalFactorCache.size() > 100 && Math.random() < 0.01) {
            personalFactorCache.entrySet().removeIf(entry -> entry.getValue().isExpired());
        }
        
        return decimals < 0 ? String.valueOf(factor) : 
               String.format("%." + decimals + "f", factor);
    }
    
    /**
     * 获取最终倍率
     */
    private String getFinalFactor(OfflinePlayer player, MarketManager market, int decimals) {
        double factor = calculateFinalFactor(player, market);
        return decimals < 0 ? String.valueOf(factor) : 
               String.format("%." + decimals + "f", factor);
    }
    
    /**
     * 计算最终倍率
     */
    private double calculateFinalFactor(OfflinePlayer player, MarketManager market) {
        double personal = 1.0;
        if (player != null && player.isOnline()) {
            // 尝试从缓存获取
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
    
    /**
     * 格式化小数
     */
    private String formatDecimal(double value, int decimals) {
        return String.format("%." + decimals + "f", value);
    }
    
    /**
     * 格式化整数 (带千分位)
     */
    private String formatInteger(double value) {
        return String.format("%,.0f", value);
    }
    
    /**
     * 格式化为 K/M 单位
     */
    private String formatThousands(double value) {
        if (value >= 1_000_000) {
            return String.format("%.1fM", value / 1_000_000);
        } else if (value >= 1_000) {
            return String.format("%.1fK", value / 1_000);
        } else {
            return String.format("%.0f", value);
        }
    }
    
    /**
     * 格式化百分比
     */
    private String formatPercent(double ratio) {
        return String.format("%.1f%%", ratio * 100);
    }
}