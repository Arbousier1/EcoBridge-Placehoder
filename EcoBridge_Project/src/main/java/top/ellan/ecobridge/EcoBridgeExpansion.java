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
        return "1.0";
    }

    @Override
    public boolean persist() {
        return true;
    }

    @Override
    public String onRequest(OfflinePlayer player, @NotNull String params) {
        // 获取 MarketManager 实例
        MarketManager market = plugin.getMarketManager();
        
        // 1. 基础经济指标
        if (params.equalsIgnoreCase("inflation")) {
            return String.format("%.4f", market.getInflation());
        }
        if (params.equalsIgnoreCase("flux")) {
            return String.format("%.4f", market.getMarketFlux());
        }
        if (params.equalsIgnoreCase("activity")) {
            return String.format("%.4f", market.getActivity());
        }
        // [新增] 财富门槛 (显示为整数)
        if (params.equalsIgnoreCase("threshold")) {
            return String.format("%.0f", market.getCurrentThreshold());
        }
        // [新增] 节假日系数
        if (params.equalsIgnoreCase("holiday")) {
            return String.format("%.2f", market.getHolidayFactor());
        }

        // 2. 动态 PID 变量 (用法: %eco_pid_shopId_productId%)
        // 例如: %eco_pid_market_diamond%
        if (params.startsWith("pid_")) {
            // 性能优化: 使用 substring(4) 替代 replace("pid_", "") 减少正则开销
            String id = params.substring(4); 
            return String.format("%.5f", plugin.getPidController().getCachedResult(id));
        }

        return null;
    }
}