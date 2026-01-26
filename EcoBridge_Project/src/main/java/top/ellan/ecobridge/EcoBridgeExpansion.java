package top.ellan.ecobridge;

import me.clip.placeholderapi.expansion.PlaceholderExpansion;
import org.bukkit.OfflinePlayer;
import org.jetbrains.annotations.NotNull;

public class EcoBridgeExpansion extends PlaceholderExpansion {
    private final EcoBridge plugin;

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
        // [完美修复] 符合 Paper 标准的写法
        // 不再调用已废弃的 plugin.getDescription()
        // 而是直接通过元数据接口获取版本字符串
        return plugin.getPluginMeta().getVersion();
    }

    @Override
    public boolean persist() {
        return true; 
    }

    @Override
    public String onRequest(OfflinePlayer player, @NotNull String params) {
        MarketManager market = plugin.getMarketManager();
        if (market == null) return "Loading...";

        // --- 1. 全局宏观指标 ---
        
        if (params.equalsIgnoreCase("inflation")) {
            return String.format("%.4f", market.getInflation());
        }
        if (params.equalsIgnoreCase("flux")) {
            return String.format("%.4f", market.getMarketFlux());
        }
        if (params.equalsIgnoreCase("activity")) {
            return String.format("%.4f", market.getActivity());
        }
        if (params.equalsIgnoreCase("threshold")) {
            return String.format("%.0f", market.getCurrentThreshold());
        }
        if (params.equalsIgnoreCase("holiday")) {
            return String.format("%.2f", market.getHolidayFactor());
        }

        // --- 2. 玩家特定指标 ---
        
        if (params.equalsIgnoreCase("personal")) {
            // calculatePersonalFactor 需要在线玩家来读取统计数据
            if (player != null && player.isOnline()) {
                return String.format("%.4f", market.calculatePersonalFactor(player.getPlayer()));
            }
            return "1.0000"; 
        }

        // [核心] 最终倍率预览
        if (params.equalsIgnoreCase("final_all")) {
            double personal = 1.0;
            if (player != null && player.isOnline()) {
                personal = market.calculatePersonalFactor(player.getPlayer());
            }
            
            double finalFactor = personal 
                               * market.getMarketFlux() 
                               * market.getHolidayFactor() 
                               * market.getActivity() 
                               * market.getInflation();
                               
            return String.format("%.4f", finalFactor);
        }

        // --- 3. PID 变量 ---
        if (params.startsWith("pid_")) {
            // 截取 "pid_" 后的 ID
            String id = params.substring(4); 
            double lambda = plugin.getPidController().getCachedResult(id);
            return String.format("%.5f", lambda);
        }

        return null;
    }
}