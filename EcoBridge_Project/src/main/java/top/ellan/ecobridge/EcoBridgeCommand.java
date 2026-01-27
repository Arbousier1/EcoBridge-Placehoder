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
import java.util.Random;
import java.util.stream.Collectors;

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
        msg(sender, "  <gold>/eb reload <dark_gray>• <gray>热重载配置、商店、市场、PID 及 Web 参数 ");
        msg(sender, "  <gold>/eb check [玩家] <dark_gray>• <gray>分析指定玩家的经济因子");
        msg(sender, "  <gold>/eb inspect <ID> <dark_gray>• <gray>实时审查物品 PID 运行数据");
        msg(sender, "  <gold>/eb save <dark_gray>• <gray>强制刷写脏数据至数据库");
        msg(sender, "");
        msg(sender, " <yellow>高级诊断:");
        msg(sender, "  <gold>/eb perf <dark_gray>• <gray>查看内存、TPS 及缓存统计");
        msg(sender, "  <gold>/eb simd <dark_gray>• <gray>CPU 向量化指令集兼容性诊断");
        msg(sender, "  <gold>/eb health <dark_gray>• <gray>系统模块健康度自动化检查");
        msg(sender, "  <gold>/eb benchmark confirm <dark_gray>• <gray>执行 5M 次 PID 压力基准测试");
        msg(sender, "<dark_gray><st>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    }

    private void handleReload(CommandSender sender) {
        msg(sender, "<yellow>⟳ 正在重载系统模块...");
        Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
            long start = System.currentTimeMillis();
            try {
                plugin.reloadConfig();

                // [Fix] 重载 WebManager 配置 (支持热更新 node-api-url)
                if (plugin.getWebManager() != null) {
                    plugin.getWebManager().reloadConfig();
                }

                // 1. 重载 PID 参数 (纯配置读取，异步安全)
                if (plugin.getPidController() != null) {
                    plugin.getPidController().reloadConfig();
                }

                // 2. [修复] 同步商店数据必须回主线程 (涉及 Bukkit API)
                Bukkit.getScheduler().runTask(plugin, () -> {
                    if (plugin.getIntegrationManager() != null) {
                        plugin.getIntegrationManager().syncShops();
                    }
                });

                // 3. 重载其他模块 (HTTP 请求/纯计算，异步安全)
                if (plugin.getMarketManager() != null) {
                    plugin.getMarketManager().updateHolidayCache();
                    plugin.getMarketManager().updateEconomyMetrics();
                    plugin.getMarketManager().updateMarketFlux();
                }

                msg(sender, "<green>✓ 重载成功! 耗时: <white>" + (System.currentTimeMillis() - start) + "ms");
            } catch (Exception e) {
                msg(sender, "<red>✗ 重载失败: " + e.getMessage());
                e.printStackTrace();
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
        if (plugin.getPidController() != null) {
            msg(sender, " <gray>控制器缓存: <aqua>" + plugin.getPidController().getCacheSize() + " <dark_gray>items");
            msg(sender, " <gray>待刷写数据: <red>" + plugin.getPidController().getDirtyQueueSize() + " <dark_gray>pending");
        }
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

        boolean db = plugin.getDatabaseManager() != null &&
                     plugin.getDatabaseManager().getDataSource() != null &&
                     !plugin.getDatabaseManager().getDataSource().isClosed();
        msg(sender, " <gray>数据库连接: " + (db ? "<green>健康" : "<red>离线"));

        int shops = (plugin.getIntegrationManager() != null) ? plugin.getIntegrationManager().getMonitoredItems().size() : 0;
        msg(sender, " <gray>商店映射: <white>" + (shops > 0 ? "<green>已挂载 (" + shops + ")" : "<red>无数据"));

        int pidItems = (plugin.getPidController() != null) ? plugin.getPidController().getCacheSize() : 0;
        msg(sender, " <gray>计算内核: " + (pidItems > 0 ? "<green>活动中" : "<yellow>空闲"));

        msg(sender, "<dark_gray><st>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    }

    private void handleInspect(CommandSender sender, String[] args) {
        if (args.length < 2) {
            msg(sender, "<red>用法: /eb inspect <shopId_productId>");
            return;
        }
        if (plugin.getPidController() == null) {
            msg(sender, "<red>✗ PID 控制器未初始化。");
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
        if (mm == null) {
            msg(sender, "<red>✗ 市场管理器未初始化。");
            return;
        }
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

        msg(sender, "<aqua>正在启动 5,000,000 次 PID 模拟迭代压力测试...（异步执行）");
        Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
            try {
                PidController pid = plugin.getPidController();
                if (pid == null) return;
                
                Random rand = new Random();
                int totalIterations = 5_000_000;
                int batchSize = 4096;
                long startNs = System.nanoTime();

                int[] handles = new int[batchSize];
                double[] volumes = new double[batchSize];

                // 填充随机有效 handle（使用缓存中的物品）
                if (plugin.getIntegrationManager() == null) return;
                List<String> monitored = plugin.getIntegrationManager().getMonitoredItems();
                if (monitored.isEmpty()) {
                    msg(sender, "<red>✗ 没有可用于基准测试的物品数据。");
                    return;
                }

                for (int i = 0; i < batchSize; i++) {
                    String id = monitored.get(i % monitored.size());
                    handles[i] = pid.getHandle(id);
                    volumes[i] = 800 + rand.nextDouble() * 400; // 模拟交易量波动
                }

                int batches = totalIterations / batchSize;
                for (int b = 0; b < batches; b++) {
                    pid.calculateBatch(handles, volumes, batchSize);
                }

                long elapsedNs = System.nanoTime() - startNs;
                double elapsedMs = elapsedNs / 1_000_000.0;
                double opsPerSec = totalIterations / (elapsedNs / 1_000_000_000.0);

                msg(sender, "<dark_gray><st>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
                msg(sender, "<gradient:aqua:blue>Benchmark 结果</gradient>");
                msg(sender, " <gray>总迭代次数: <white>" + totalIterations);
                msg(sender, String.format(" <gray>耗时: <white>%.2f ms", elapsedMs));
                msg(sender, String.format(" <gray>吞吐量: <gold>%.0f ops/s", opsPerSec));
                msg(sender, " <gray>平均每批: <white>" + String.format("%.2f ms", elapsedMs / batches));
                msg(sender, "<dark_gray><st>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

            } catch (Exception e) {
                msg(sender, "<red>✗ Benchmark 执行失败: " + e.getMessage());
                e.printStackTrace();
            }
        });
    }

    private void handleSave(CommandSender sender) {
        if (plugin.getPidController() == null) {
            msg(sender, "<gray>PID 模块未加载。");
            return;
        }
        int dirty = plugin.getPidController().getDirtyQueueSize();
        if (dirty == 0) {
            msg(sender, "<gray>没有需要刷写的脏数据。");
            return;
        }
        msg(sender, "<yellow>正在手动刷写 " + dirty + " 条数据...");
        plugin.getPidController().flushBuffer(true); // 同步刷写
        msg(sender, "<green>数据已安全持久化至数据库。");
    }

    @Override
    public @Nullable List<String> onTabComplete(@NotNull CommandSender sender, @NotNull Command command, @NotNull String label, @NotNull String[] args) {
        if (args.length == 1) {
            return filter(args[0], Arrays.asList("reload", "check", "inspect", "save", "perf", "simd", "health", "benchmark", "help"));
        }
        if (args.length == 2) {
            if (args[0].equalsIgnoreCase("check")) return null;
            if (args[0].equalsIgnoreCase("inspect") && plugin.getIntegrationManager() != null) {
                return filter(args[1], plugin.getIntegrationManager().getMonitoredItems());
            }
        }
        return Collections.emptyList();
    }

    private List<String> filter(String input, List<String> list) {
        return list.stream().filter(s -> s.toLowerCase().startsWith(input.toLowerCase())).collect(Collectors.toList());
    }
}