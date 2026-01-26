package top.ellan.ecobridge;

import net.kyori.adventure.text.minimessage.MiniMessage;
import org.bukkit.Bukkit;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.command.TabCompleter;
import org.bukkit.entity.Player;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class EcoBridgeCommand implements CommandExecutor, TabCompleter {

    private final EcoBridge plugin;
    // [优化] 预创建 MiniMessage 实例，避免重复创建开销
    private final MiniMessage mm = MiniMessage.miniMessage();

    public EcoBridgeCommand(EcoBridge plugin) {
        this.plugin = plugin;
    }

    // [新增] 辅助方法：发送解析后的 MiniMessage 消息
    // 支持 <red>, <gradient:red:blue> 等高级格式
    private void msg(CommandSender sender, String message) {
        sender.sendMessage(mm.deserialize(message));
    }

    @Override
    public boolean onCommand(@NotNull CommandSender sender, @NotNull Command command, @NotNull String label, @NotNull String[] args) {
        if (!sender.hasPermission("ecobridge.admin")) {
            msg(sender, "<red>You do not have permission.");
            return true;
        }

        if (args.length == 0 || args[0].equalsIgnoreCase("help")) {
            sendHelp(sender);
            return true;
        }

        String sub = args[0].toLowerCase();

        // [优化] Java 14+ Switch 表达式，更简洁
        switch (sub) {
            case "reload" -> handleReload(sender);
            case "check" -> handleCheck(sender, args);
            case "perf" -> handlePerf(sender);
            case "save" -> handleSave(sender);
            case "inspect" -> handleInspect(sender, args);
            default -> sendHelp(sender);
        }
        return true;
    }

    private void sendHelp(CommandSender sender) {
        // <st> = 删除线, <bold> = 加粗
        msg(sender, "<dark_gray><st>----------------------------------------");
        // [修复] 使用 getPluginMeta().getVersion() 替代废弃的 getDescription()
        msg(sender, "<gold><bold>EcoBridge <gray>v" + plugin.getPluginMeta().getVersion());
        msg(sender, "<yellow>/eb reload <gray>- 重载配置与缓存");
        msg(sender, "<yellow>/eb check [玩家] <gray>- 查看玩家经济因子");
        msg(sender, "<yellow>/eb inspect <ID> <gray>- 查看物品PID状态");
        msg(sender, "<yellow>/eb save <gray>- 强制刷写缓冲区");
        msg(sender, "<yellow>/eb perf <gray>- 查看性能监控");
        msg(sender, "<dark_gray><st>----------------------------------------");
    }

    private void handleReload(CommandSender sender) {
        msg(sender, "<yellow>[EcoBridge] 正在执行异步热重载...");
        Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
            try {
                plugin.reloadConfig();
                plugin.getIntegrationManager().syncShops();
                plugin.getMarketManager().updateHolidayCache();
                plugin.getMarketManager().updateEconomyMetrics();
                plugin.getMarketManager().updateMarketFlux();
                msg(sender, "<green>[EcoBridge] 核心系统重载完毕 (Java Native).");
            } catch (Exception e) {
                msg(sender, "<red>重载失败: " + e.getMessage());
                e.printStackTrace();
            }
        });
    }

    private void handleSave(CommandSender sender) {
        int size = plugin.getPidController().getDirtyQueueSize();
        msg(sender, "<yellow>[EcoBridge] 正在强制刷写 <red>" + size + " <yellow>条数据...");
        plugin.getPidController().flushBuffer(false); 
        msg(sender, "<green>[EcoBridge] 保存任务已提交。");
    }

    private void handlePerf(CommandSender sender) {
        Runtime rt = Runtime.getRuntime();
        double totalMem = rt.totalMemory() / 1048576.0;
        double freeMem = rt.freeMemory() / 1048576.0;
        double usedMem = totalMem - freeMem;
        
        double tps = Bukkit.getTPS()[0];
        // 动态颜色 Tag
        String tpsColor = (tps > 18) ? "<green>" : (tps > 15 ? "<yellow>" : "<red>");

        msg(sender, "<dark_gray><st>----------------------------------------");
        msg(sender, "<gold>EcoBridge <gray>>> <white>性能监控面板");
        msg(sender, " <gray>[Cache Stats]");
        msg(sender, "<yellow> PID Cache: <aqua>" + plugin.getPidController().getCacheSize());
        msg(sender, "<yellow> Dirty Buffer: <red>" + plugin.getPidController().getDirtyQueueSize());
        msg(sender, "");
        msg(sender, " <gray>[System]");
        msg(sender, "<yellow> TPS (1m): " + tpsColor + String.format("%.2f", tps));
        msg(sender, "<yellow> Memory: <aqua>" + String.format("%.0fMB", usedMem) + 
                         " <gray>/ <dark_gray>" + String.format("%.0fMB", totalMem));
        msg(sender, "<dark_gray><st>----------------------------------------");
    }

    private void handleCheck(CommandSender sender, String[] args) {
        Player target;
        if (args.length > 1) {
            target = Bukkit.getPlayer(args[1]);
        } else if (sender instanceof Player p) {
            target = p;
        } else {
            msg(sender, "<red>控制台必须指定玩家。");
            return;
        }

        if (target == null) {
            msg(sender, "<red>玩家不在线。");
            return;
        }

        Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
            MarketManager mm = plugin.getMarketManager();
            double personal = mm.calculatePersonalFactor(target);
            double threshold = mm.getCurrentThreshold();
            double inflation = mm.getInflation();
            double flux = mm.getMarketFlux();
            double holiday = mm.getHolidayFactor();
            double activity = mm.getActivity();
            double finalFactor = personal * flux * holiday * activity * inflation;

            msg(sender, "<dark_gray><st>----------------------------------------");
            msg(sender, "<gold>EcoBridge <gray>>> <white>玩家状态: <yellow>" + target.getName());
            msg(sender, " <yellow>[基础因子]");
            msg(sender, String.format(" <gray>- 个人系数: <aqua>%.4f", personal));
            msg(sender, String.format(" <gray>- 节假系数: <light_purple>%.2f", holiday));
            msg(sender, String.format(" <gray>- 活跃系数: <dark_aqua>%.4f", activity));
            msg(sender, "");
            msg(sender, " <yellow>[宏观经济]");
            msg(sender, String.format(" <gray>- 富人门槛: <green>%.0f", threshold));
            msg(sender, String.format(" <gray>- 通胀系数: <gold>%.4f", inflation));
            msg(sender, String.format(" <gray>- 市场波动: <dark_purple>%.4f", flux));
            msg(sender, "");
            msg(sender, " <yellow>[最终倍率预览]: <green><bold>" + String.format("%.4f", finalFactor));
            msg(sender, "<dark_gray><st>----------------------------------------");
        });
    }

    private void handleInspect(CommandSender sender, String[] args) {
        if (args.length < 2) {
            msg(sender, "<red>用法: /eb inspect <shopId_productId>");
            return;
        }
        String itemId = args[1]; 

        PidController.PidStateDto state = plugin.getPidController().inspectState(itemId);
        
        if (state == null) {
            msg(sender, "<red>未找到物品 [<white>" + itemId + "<red>] 的 PID 状态。");
            msg(sender, "<gray>请确认该物品已被监控且产生过交易。");
            return;
        }

        long secondsAgo = (System.currentTimeMillis() - state.updateTime()) / 1000;

        msg(sender, "<dark_gray><st>----------------------------------------");
        msg(sender, "<gold>PID 状态审查 <gray>>> <white>" + itemId);
        msg(sender, "<yellow> Integral (积分): <aqua>" + String.format("%.4f", state.integral()));
        msg(sender, "<yellow> Last Error (误差): <red>" + String.format("%.4f", state.lastError()));
        msg(sender, "<yellow> Last Lambda (系数): <green>" + String.format("%.5f", state.lastLambda()));
        msg(sender, "<yellow> Last Update: <gray>" + secondsAgo + "s ago");
        msg(sender, "<dark_gray><st>----------------------------------------");
    }

    @Override
    public @Nullable List<String> onTabComplete(@NotNull CommandSender sender, @NotNull Command command, @NotNull String label, @NotNull String[] args) {
        if (args.length == 1) {
            return filter(args[0], Arrays.asList("reload", "check", "inspect", "save", "perf", "help"));
        }
        if (args.length == 2 && args[0].equalsIgnoreCase("check")) {
            return null; 
        }
        if (args.length == 2 && args[0].equalsIgnoreCase("inspect")) {
            return filter(args[0], plugin.getIntegrationManager().getMonitoredItems());
        }
        return Collections.emptyList();
    }

    private List<String> filter(String input, List<String> list) {
        return list.stream().filter(s -> s.toLowerCase().startsWith(input.toLowerCase())).collect(Collectors.toList());
    }
}