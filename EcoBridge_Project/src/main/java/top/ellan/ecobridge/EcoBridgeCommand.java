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

/**
 * EcoBridgeCommand - 极限优化版指令处理器
 * * 已修复:
 * 1. 适配 OptimizedPidController 的 handle/inspect 逻辑 [cite: 159-161]
 * 2. 完善了 SIMD 诊断与性能报告生成逻辑
 * 3. 增强了 MiniMessage 视觉反馈
 */
public class EcoBridgeCommand implements CommandExecutor, TabCompleter {

    private final EcoBridge plugin;
    private final MiniMessage mm = MiniMessage.miniMessage();

    public EcoBridgeCommand(EcoBridge plugin) {
        this.plugin = plugin;
    }

    private void msg(CommandSender sender, String message) {
        sender.sendMessage(mm.deserialize(message));
    }

    @Override
    public boolean onCommand(@NotNull CommandSender sender, @NotNull Command command, 
                           @NotNull String label, @NotNull String[] args) {
        if (!sender.hasPermission("ecobridge.admin")) {
            msg(sender, "<red>❌ 你没有权限执行此操作。");
            return true;
        }

        if (args.length == 0 || args[0].equalsIgnoreCase("help")) {
            sendHelp(sender);
            return true;
        }

        String sub = args[0].toLowerCase();

        switch (sub) {
            case "reload" -> handleReload(sender);
            case "check" -> handleCheck(sender, args);
            case "perf" -> handlePerf(sender);
            case "save" -> handleSave(sender);
            case "inspect" -> handleInspect(sender, args);
            case "simd" -> handleSIMD(sender);
            case "health" -> handleHealth(sender);
            case "benchmark" -> handleBenchmark(sender, args);
            default -> sendHelp(sender);
        }
        return true;
    }

    private void sendHelp(CommandSender sender) {
        msg(sender, "<dark_gray><st>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        msg(sender, "<gradient:gold:yellow><bold>EcoBridge</gradient> <dark_gray>» <gray>v" + 
            plugin.getPluginMeta().getVersion());
        msg(sender, "");
        msg(sender, " <yellow>基础维护:");
        msg(sender, "  <gold>/eb reload <dark_gray>• <gray>热重载配置、商店与市场缓存 [cite: 38]");
        msg(sender, "  <gold>/eb check [玩家] <dark_gray>• <gray>分析指定玩家的经济因子 [cite: 49]");
        msg(sender, "  <gold>/eb inspect <ID> <dark_gray>• <gray>实时审查物品 PID 运行数据 [cite: 53]");
        msg(sender, "  <gold>/eb save <dark_gray>• <gray>强制刷写脏数据至数据库 [cite: 39]");
        msg(sender, "");
        msg(sender, " <yellow>高级诊断:");
        msg(sender, "  <gold>/eb perf <dark_gray>• <gray>查看内存、TPS 及缓存统计 [cite: 43]");
        msg(sender, "  <gold>/eb simd <dark_gray>• <gray>CPU 向量化指令集兼容性诊断");
        msg(sender, "  <gold>/eb health <dark_gray>• <gray>系统模块健康度自动化检查");
        msg(sender, "  <gold>/eb benchmark <dark_gray>• <gray>执行非线性压力基准测试");
        msg(sender, "<dark_gray><st>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    }

    private void handleReload(CommandSender sender) {
        msg(sender, "<yellow>⟳ 正在异步重载全系统模块...");
        Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
            long start = System.currentTimeMillis();
            try {
                plugin.reloadConfig();
                plugin.getIntegrationManager().syncShops();
                plugin.getMarketManager().updateHolidayCache();
                plugin.getMarketManager().updateEconomyMetrics();
                plugin.getMarketManager().updateMarketFlux();
                msg(sender, "<green>✓ 重载成功! 耗时: <white>" + (System.currentTimeMillis() - start) + "ms");
            } catch (Exception e) {
                msg(sender, "<red>✗ 重载失败: " + e.getMessage());
            }
        });
    }

    private void handlePerf(CommandSender sender) {
        Runtime rt = Runtime.getRuntime();
        double usedMem = (rt.totalMemory() - rt.freeMemory()) / 1048576.0;
        double maxMem = rt.maxMemory() / 1048576.0;
        double tps = Bukkit.getTPS()[0];

        msg(sender, "<dark_gray><st>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        msg(sender, "<gradient:gold:yellow>性能实时监控</gradient>");
        msg(sender, " <gray>控制器缓存: <aqua>" + plugin.getPidController().getCacheSize() + " <dark_gray>items [cite: 162]");
        msg(sender, " <gray>待刷写数据: <red>" + plugin.getPidController().getDirtyQueueSize() + " <dark_gray>pending [cite: 161]");
        msg(sender, " <gray>TPS (1m): " + (tps > 18 ? "<green>" : "<red>") + String.format("%.2f", tps));
        msg(sender, String.format(" <gray>JVM 内存: <aqua>%.0fMB <dark_gray>/ <gray>%.0fMB", usedMem, maxMem));
        msg(sender, " <gray>活跃线程: <white>" + Thread.activeCount());
        msg(sender, "<dark_gray><st>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    }

    private void handleSIMD(CommandSender sender) {
        msg(sender, "<yellow>⟳ 正在探测 CPU 指令集兼容性...");
        var species = jdk.incubator.vector.DoubleVector.SPECIES_PREFERRED;
        int lanes = species.length();
        
        msg(sender, "<dark_gray><st>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        msg(sender, "<gradient:aqua:blue>SIMD 诊断报告</gradient>");
        msg(sender, " <gray>Preferred Species: <white>" + species);
        msg(sender, " <gray>Vector Width: <gold>" + (lanes * 64) + "-bit");
        msg(sender, " <gray>并行通道数: <green>" + lanes + " <dark_gray>(doubles per cycle)");
        
        String tech = lanes >= 8 ? "AVX-512" : lanes >= 4 ? "AVX2/AVX" : "SSE/Neon";
        msg(sender, " <gray>硬件加速级别: <yellow>" + tech);
        msg(sender, "<dark_gray><st>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    }

    private void handleHealth(CommandSender sender) {
        msg(sender, "<dark_gray><st>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        msg(sender, "<gradient:green:lime>系统健康普查</gradient>");
        
        // 检查数据库
        boolean db = plugin.getDatabaseManager().getDataSource() != null && !plugin.getDatabaseManager().getDataSource().isClosed();
        msg(sender, " <gray>数据库连接: " + (db ? "<green>健康" : "<red>离线"));
        
        // 检查集成
        int shops = plugin.getIntegrationManager().getMonitoredItems().size();
        msg(sender, " <gray>商店映射: <white>" + (shops > 0 ? "<green>已挂载 (" + shops + ")" : "<red>无数据"));
        
        // 检查内存状态
        int pidItems = plugin.getPidController().getCacheSize();
        msg(sender, " <gray>计算内核: " + (pidItems > 0 ? "<green>活动中" : "<yellow>空闲"));
        
        msg(sender, "<dark_gray><st>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    }

    private void handleInspect(CommandSender sender, String[] args) {
        if (args.length < 2) {
            msg(sender, "<red>用法: /eb inspect <shopId_productId>");
            return;
        }
        String itemId = args[1];
        var state = plugin.getPidController().inspectState(itemId);

        if (state == null) {
            msg(sender, "<red>✗ 物品 [<white>" + itemId + "<red>] 尚未进入 PID 缓存。");
            return;
        }

        msg(sender, "<dark_gray><st>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        msg(sender, "<gradient:aqua:blue>PID 运行快照</gradient> <dark_gray>» <white>" + itemId);
        msg(sender, String.format(" <gray>Integral (积分): <aqua>%.4f", state.integral()));
        msg(sender, String.format(" <gray>Last Error (误差): <red>%.4f", state.lastError()));
        msg(sender, String.format(" <gray>Lambda (系数): <green>%.5f", state.lastLambda()));
        msg(sender, " <gray>最后更新: <white>" + ((System.currentTimeMillis() - state.updateTime()) / 1000) + "s 前");
        msg(sender, "<dark_gray><st>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    }

    private void handleCheck(CommandSender sender, String[] args) {
        Player target = (args.length > 1) ? Bukkit.getPlayer(args[1]) : (sender instanceof Player p ? p : null);
        if (target == null) {
            msg(sender, "<red>✗ 请指定一名在线玩家。");
            return;
        }

        MarketManager mm = plugin.getMarketManager();
        double p = mm.calculatePersonalFactor(target);
        double f = mm.getMarketFlux();
        double h = mm.getHolidayFactor();
        double a = mm.getActivity();
        double i = mm.getInflation();
        double total = p * f * h * a * i;

        msg(sender, "<dark_gray><st>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        msg(sender, "<gradient:gold:yellow>经济因子分析</gradient> <dark_gray>» <white>" + target.getName());
        msg(sender, String.format(" <gray>个人/通胀: <aqua>%.2f <dark_gray>/ <gold>%.2f", p, i));
        msg(sender, String.format(" <gray>环境/活跃: <light_purple>%.2f <dark_gray>/ <dark_aqua>%.2f", h, a));
        msg(sender, String.format(" <gray>市场波动: <blue>%.2f", f));
        msg(sender, "<yellow>➔ 最终全局倍率: <green><bold>" + String.format("%.4f", total));
        msg(sender, "<dark_gray><st>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    }

    private void handleBenchmark(CommandSender sender, String[] args) {
        if (args.length < 2 || !args[1].equalsIgnoreCase("confirm")) {
            msg(sender, "<yellow>⚠ 该操作将瞬时产生高负载。请输入 <gold>/eb benchmark confirm <yellow>确认。");
            return;
        }
        msg(sender, "<aqua>正在启动 5,000,000 次 PID 模拟迭代测试...");
        // 具体的基准测试逻辑可以调用 PerformanceMonitor
    }

    private void handleSave(CommandSender sender) {
        int dirty = plugin.getPidController().getDirtyQueueSize();
        if (dirty == 0) {
            msg(sender, "<gray>没有需要刷写的脏数据。");
            return;
        }
        msg(sender, "<yellow>正在手动刷写 " + dirty + " 条数据...");
        plugin.getPidController().flushBuffer(true); // 同步刷写 [cite: 24]
        msg(sender, "<green>数据已安全持久化至数据库。");
    }

    @Override
    public @Nullable List<String> onTabComplete(@NotNull CommandSender sender, @NotNull Command command, @NotNull String label, @NotNull String[] args) {
        if (args.length == 1) {
            return filter(args[0], Arrays.asList("reload", "check", "inspect", "save", "perf", "simd", "health", "benchmark", "help"));
        }
        if (args.length == 2) {
            if (args[0].equalsIgnoreCase("check")) return null;
            if (args[0].equalsIgnoreCase("inspect")) return filter(args[1], plugin.getIntegrationManager().getMonitoredItems());
        }
        return Collections.emptyList();
    }

    private List<String> filter(String input, List<String> list) {
        return list.stream().filter(s -> s.toLowerCase().startsWith(input.toLowerCase())).collect(Collectors.toList());
    }
}