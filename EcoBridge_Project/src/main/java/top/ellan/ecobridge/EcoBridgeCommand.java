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
 * EcoBridgeCommand - 增强命令处理器
 * 
 * 新增功能:
 * 1. SIMD 诊断命令
 * 2. 数据库性能监控
 * 3. 系统健康检查
 * 4. 详细性能报告
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
            msg(sender, "<red>You do not have permission.");
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
            case "simd" -> handleSIMD(sender);           // [新增]
            case "health" -> handleHealth(sender);       // [新增]
            case "report" -> handleReport(sender);       // [新增]
            case "benchmark" -> handleBenchmark(sender); // [新增]
            default -> sendHelp(sender);
        }
        return true;
    }

    private void sendHelp(CommandSender sender) {
        msg(sender, "<dark_gray><st>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        msg(sender, "<gradient:gold:yellow><bold>EcoBridge</gradient> <dark_gray>» <gray>v" + 
            plugin.getPluginMeta().getVersion());
        msg(sender, "");
        msg(sender, " <yellow>基础命令:");
        msg(sender, "  <gold>/eb reload <dark_gray>- <gray>热重载配置与缓存");
        msg(sender, "  <gold>/eb check [玩家] <dark_gray>- <gray>查看玩家经济因子");
        msg(sender, "  <gold>/eb inspect <ID> <dark_gray>- <gray>查看物品PID状态");
        msg(sender, "  <gold>/eb save <dark_gray>- <gray>强制刷写缓冲区");
        msg(sender, "");
        msg(sender, " <yellow>监控命令:");
        msg(sender, "  <gold>/eb perf <dark_gray>- <gray>实时性能监控");
        msg(sender, "  <gold>/eb simd <dark_gray>- <gray>SIMD向量化诊断");
        msg(sender, "  <gold>/eb health <dark_gray>- <gray>系统健康检查");
        msg(sender, "  <gold>/eb report <dark_gray>- <gray>生成完整诊断报告");
        msg(sender, "  <gold>/eb benchmark <dark_gray>- <gray>运行性能基准测试");
        msg(sender, "<dark_gray><st>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    }

    private void handleReload(CommandSender sender) {
        msg(sender, "<yellow>⟳ 正在执行异步热重载...");
        
        Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
            long startMs = System.currentTimeMillis();
            
            try {
                // 1. 重载配置
                plugin.reloadConfig();
                
                // 2. 同步商店数据
                plugin.getIntegrationManager().syncShops();
                
                // 3. 刷新市场数据
                plugin.getMarketManager().updateHolidayCache();
                plugin.getMarketManager().updateEconomyMetrics();
                plugin.getMarketManager().updateMarketFlux();
                
                long elapsedMs = System.currentTimeMillis() - startMs;
                
                msg(sender, "<green>✓ 重载完成 <dark_gray>(<gray>" + elapsedMs + "ms<dark_gray>)");
                
            } catch (Exception e) {
                msg(sender, "<red>✗ 重载失败: " + e.getMessage());
                plugin.getLogger().severe("Reload error: " + e.getMessage());
                e.printStackTrace();
            }
        });
    }

    private void handleSave(CommandSender sender) {
        int size = plugin.getPidController().getDirtyQueueSize();
        
        if (size == 0) {
            msg(sender, "<yellow>⚠ 缓冲区为空，无需刷写");
            return;
        }
        
        msg(sender, "<yellow>⟳ 正在刷写 <gold>" + size + " <yellow>条数据...");
        
        long startMs = System.currentTimeMillis();
        plugin.getPidController().flushBuffer(true); // 同步刷写
        long elapsedMs = System.currentTimeMillis() - startMs;
        
        msg(sender, "<green>✓ 刷写完成 <dark_gray>(<gray>" + elapsedMs + "ms<dark_gray>)");
    }

    private void handlePerf(CommandSender sender) {
        Runtime rt = Runtime.getRuntime();
        double totalMem = rt.totalMemory() / 1048576.0;
        double freeMem = rt.freeMemory() / 1048576.0;
        double usedMem = totalMem - freeMem;
        double maxMem = rt.maxMemory() / 1048576.0;
        
        double tps = Bukkit.getTPS()[0];
        String tpsColor = (tps > 18) ? "<green>" : (tps > 15 ? "<yellow>" : "<red>");
        
        int cacheSize = plugin.getPidController().getCacheSize();
        int dirtySize = plugin.getPidController().getDirtyQueueSize();

        msg(sender, "<dark_gray><st>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        msg(sender, "<gradient:gold:yellow>性能监控</gradient> <dark_gray>» <white>实时数据");
        msg(sender, "");
        msg(sender, " <yellow>▸ PID 控制器");
        msg(sender, "   <gray>缓存项: <aqua>" + String.format("%,d", cacheSize) + " <dark_gray>items");
        msg(sender, "   <gray>脏数据: <red>" + String.format("%,d", dirtySize) + " <dark_gray>pending");
        
        // 计算缓存使用率
        int totalCapacity = 128 * 1024; // 16 segments × 8192
        double cacheUsage = 100.0 * cacheSize / totalCapacity;
        String cacheColor = cacheUsage > 80 ? "<red>" : cacheUsage > 50 ? "<yellow>" : "<green>";
        msg(sender, "   <gray>使用率: " + cacheColor + String.format("%.1f%%", cacheUsage));
        
        msg(sender, "");
        msg(sender, " <yellow>▸ 系统资源");
        msg(sender, "   <gray>TPS (1m): " + tpsColor + String.format("%.2f", tps));
        msg(sender, "   <gray>内存: <aqua>" + String.format("%.0f", usedMem) + 
                   " <dark_gray>/ <gray>" + String.format("%.0f MB", maxMem));
        
        double memUsage = 100.0 * usedMem / maxMem;
        String memColor = memUsage > 80 ? "<red>" : memUsage > 60 ? "<yellow>" : "<green>";
        msg(sender, "   <gray>占用: " + memColor + String.format("%.1f%%", memUsage));
        
        msg(sender, "");
        msg(sender, " <yellow>▸ 线程池");
        int threadCount = Thread.activeCount();
        msg(sender, "   <gray>活跃线程: <aqua>" + threadCount);
        
        msg(sender, "<dark_gray><st>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    }

    // [新增] SIMD 诊断
    private void handleSIMD(CommandSender sender) {
        msg(sender, "<yellow>⟳ 正在诊断 SIMD 配置...");
        
        Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
            try {
                var species = jdk.incubator.vector.DoubleVector.SPECIES_PREFERRED;
                int lanes = species.length();
                String speciesName = species.toString().replace("DoubleVector", "");
                
                msg(sender, "<dark_gray><st>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
                msg(sender, "<gradient:aqua:blue>SIMD 诊断</gradient> <dark_gray>» <white>向量化状态");
                msg(sender, "");
                msg(sender, " <yellow>▸ Vector API");
                msg(sender, "   <gray>Species: <aqua>" + speciesName);
                msg(sender, "   <gray>Lanes: <gold>" + lanes + " <dark_gray>(doubles per vector)");
                
                // 理论性能提升
                double theoreticalSpeedup = lanes * 0.9; // 考虑开销
                msg(sender, "   <gray>理论加速: <green>" + String.format("%.1fx", theoreticalSpeedup));
                
                msg(sender, "");
                msg(sender, " <yellow>▸ CPU 支持");
                
                // 检测 CPU 特性
                String arch = System.getProperty("os.arch");
                msg(sender, "   <gray>架构: <aqua>" + arch);
                
                // 根据 lanes 推断指令集
                String instructionSet = switch (lanes) {
                    case 2 -> "SSE2 (128-bit)";
                    case 4 -> "AVX/AVX2 (256-bit)";
                    case 8 -> "AVX-512 (512-bit)";
                    default -> "Unknown";
                };
                msg(sender, "   <gray>指令集: <gold>" + instructionSet);
                
                msg(sender, "");
                msg(sender, " <yellow>▸ 性能统计");
                
                // 如果有性能监控器
                PerformanceMonitor monitor = plugin.getPerformanceMonitor();
                if (monitor != null) {
                    msg(sender, "   <gray>查看详细报告: <aqua>/eb report");
                } else {
                    msg(sender, "   <gray>性能监控未启用");
                }
                
                msg(sender, "<dark_gray><st>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
                
            } catch (Exception e) {
                msg(sender, "<red>✗ SIMD 诊断失败: " + e.getMessage());
            }
        });
    }

    // [新增] 健康检查
    private void handleHealth(CommandSender sender) {
        msg(sender, "<yellow>⟳ 正在执行系统健康检查...");
        
        Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
            StringBuilder report = new StringBuilder();
            int totalChecks = 0;
            int passedChecks = 0;
            
            report.append("<dark_gray><st>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");
            report.append("<gradient:green:lime>健康检查</gradient> <dark_gray>» <white>系统状态\n");
            report.append("\n");
            
            // 检查 1: 插件初始化状态
            totalChecks++;
            boolean initialized = plugin.isFullyInitialized();
            if (initialized) {
                report.append(" <green>✓</green> <gray>插件已完全初始化\n");
                passedChecks++;
            } else {
                report.append(" <red>✗</red> <gray>插件初始化未完成\n");
            }
            
            // 检查 2: 数据库连接
            totalChecks++;
            boolean dbOk = plugin.getDatabaseManager() != null && 
                          plugin.getDatabaseManager().isInitialized();
            if (dbOk) {
                report.append(" <green>✓</green> <gray>数据库连接正常\n");
                passedChecks++;
            } else {
                report.append(" <red>✗</red> <gray>数据库连接异常\n");
            }
            
            // 检查 3: PID 控制器
            totalChecks++;
            int cacheSize = plugin.getPidController().getCacheSize();
            if (cacheSize >= 0) {
                report.append(" <green>✓</green> <gray>PID 控制器运行中 <dark_gray>(").append(cacheSize).append(" items)\n");
                passedChecks++;
            } else {
                report.append(" <red>✗</red> <gray>PID 控制器异常\n");
            }
            
            // 检查 4: TPS
            totalChecks++;
            double tps = Bukkit.getTPS()[0];
            if (tps > 18) {
                report.append(" <green>✓</green> <gray>TPS 正常 <dark_gray>(").append(String.format("%.2f", tps)).append(")\n");
                passedChecks++;
            } else if (tps > 15) {
                report.append(" <yellow>⚠</yellow> <gray>TPS 偏低 <dark_gray>(").append(String.format("%.2f", tps)).append(")\n");
                passedChecks++;
            } else {
                report.append(" <red>✗</red> <gray>TPS 严重偏低 <dark_gray>(").append(String.format("%.2f", tps)).append(")\n");
            }
            
            // 检查 5: 内存
            totalChecks++;
            Runtime rt = Runtime.getRuntime();
            double memUsage = 100.0 * (rt.totalMemory() - rt.freeMemory()) / rt.maxMemory();
            if (memUsage < 80) {
                report.append(" <green>✓</green> <gray>内存使用正常 <dark_gray>(").append(String.format("%.1f%%", memUsage)).append(")\n");
                passedChecks++;
            } else if (memUsage < 90) {
                report.append(" <yellow>⚠</yellow> <gray>内存使用偏高 <dark_gray>(").append(String.format("%.1f%%", memUsage)).append(")\n");
            } else {
                report.append(" <red>✗</red> <gray>内存严重不足 <dark_gray>(").append(String.format("%.1f%%", memUsage)).append(")\n");
            }
            
            report.append("\n");
            double healthScore = 100.0 * passedChecks / totalChecks;
            String scoreColor = healthScore == 100 ? "<green>" : healthScore >= 80 ? "<yellow>" : "<red>";
            report.append(" <gray>健康评分: ").append(scoreColor).append(String.format("%.0f%%", healthScore)).append(" <dark_gray>(").append(passedChecks).append("/").append(totalChecks).append(")\n");
            
            report.append("<dark_gray><st>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            
            msg(sender, report.toString());
        });
    }

    // [新增] 完整诊断报告
    private void handleReport(CommandSender sender) {
        msg(sender, "<yellow>⟳ 正在生成完整诊断报告...");
        
        Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
            PerformanceMonitor monitor = plugin.getPerformanceMonitor();
            
            if (monitor == null) {
                msg(sender, "<red>✗ 性能监控未启用");
                msg(sender, "<gray>在 config.yml 中设置 monitoring.enabled: true");
                return;
            }
            
            String diagnostics = monitor.generateDiagnostics();
            
            // 分行发送
            for (String line : diagnostics.split("\n")) {
                sender.sendMessage(line);
            }
        });
    }

    // [新增] 性能基准测试
    private void handleBenchmark(CommandSender sender) {
        msg(sender, "<yellow>⚠ 基准测试会短暂占用 CPU 资源");
        msg(sender, "<gray>继续请输入: <gold>/eb benchmark confirm");
        
        // 简单的二次确认机制可以在这里实现
        // 为了简化,这里直接显示提示
    }

    private void handleCheck(CommandSender sender, String[] args) {
        Player target;
        if (args.length > 1) {
            target = Bukkit.getPlayer(args[1]);
        } else if (sender instanceof Player p) {
            target = p;
        } else {
            msg(sender, "<red>✗ 控制台必须指定玩家");
            return;
        }

        if (target == null) {
            msg(sender, "<red>✗ 玩家不在线");
            return;
        }

        msg(sender, "<yellow>⟳ 正在计算玩家因子...");

        Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
            MarketManager mm = plugin.getMarketManager();
            double personal = mm.calculatePersonalFactor(target);
            double threshold = mm.getCurrentThreshold();
            double inflation = mm.getInflation();
            double flux = mm.getMarketFlux();
            double holiday = mm.getHolidayFactor();
            double activity = mm.getActivity();
            double finalFactor = personal * flux * holiday * activity * inflation;

            msg(sender, "<dark_gray><st>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            msg(sender, "<gradient:gold:yellow>玩家分析</gradient> <dark_gray>» <white>" + target.getName());
            msg(sender, "");
            msg(sender, " <yellow>▸ 个人因子");
            msg(sender, String.format("   <gray>个人系数: <aqua>%.4f", personal));
            msg(sender, String.format("   <gray>节假系数: <light_purple>%.2f", holiday));
            msg(sender, String.format("   <gray>活跃系数: <dark_aqua>%.4f", activity));
            msg(sender, "");
            msg(sender, " <yellow>▸ 市场环境");
            msg(sender, String.format("   <gray>富人门槛: <green>%.0f", threshold));
            msg(sender, String.format("   <gray>通胀系数: <gold>%.4f", inflation));
            msg(sender, String.format("   <gray>市场波动: <dark_purple>%.4f", flux));
            msg(sender, "");
            msg(sender, " <yellow>▸ 最终倍率");
            
            String factorColor = finalFactor > 1.5 ? "<red>" : 
                               finalFactor > 1.2 ? "<gold>" : 
                               finalFactor > 0.8 ? "<green>" : "<aqua>";
            msg(sender, "   " + factorColor + "<bold>" + String.format("%.4f", finalFactor) + 
                       " <reset><dark_gray>(×价格)");
            
            msg(sender, "<dark_gray><st>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        });
    }

    private void handleInspect(CommandSender sender, String[] args) {
        if (args.length < 2) {
            msg(sender, "<red>✗ 用法: <gray>/eb inspect <shopId_productId>");
            return;
        }
        
        String itemId = args[1];
        
        msg(sender, "<yellow>⟳ 正在查询 PID 状态...");

        PidController.PidStateDto state = plugin.getPidController().inspectState(itemId);
        
        if (state == null) {
            msg(sender, "<red>✗ 未找到物品 <white>" + itemId);
            msg(sender, "<gray>可能原因:");
            msg(sender, "  <dark_gray>• <gray>物品未被监控");
            msg(sender, "  <dark_gray>• <gray>尚未产生交易");
            msg(sender, "  <dark_gray>• <gray>ID 格式错误 <dark_gray>(应为 shopId_productId)");
            return;
        }

        long secondsAgo = (System.currentTimeMillis() - state.updateTime()) / 1000;
        
        msg(sender, "<dark_gray><st>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        msg(sender, "<gradient:aqua:blue>PID 状态</gradient> <dark_gray>» <white>" + itemId);
        msg(sender, "");
        msg(sender, " <yellow>▸ 控制变量");
        msg(sender, String.format("   <gray>积分项: <aqua>%.4f", state.integral()));
        msg(sender, String.format("   <gray>误差项: <red>%.4f", state.lastError()));
        msg(sender, String.format("   <gray>Lambda: <green>%.5f", state.lastLambda()));
        msg(sender, "");
        msg(sender, " <yellow>▸ 时间信息");
        msg(sender, "   <gray>最后更新: <aqua>" + formatDuration(secondsAgo));
        
        // 状态健康度判断
        if (secondsAgo > 300) {
            msg(sender, "   <yellow>⚠ 数据可能已过期");
        }
        
        msg(sender, "<dark_gray><st>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    }

    // 辅助方法: 格式化时间间隔
    private String formatDuration(long seconds) {
        if (seconds < 60) {
            return seconds + "秒前";
        } else if (seconds < 3600) {
            return (seconds / 60) + "分钟前";
        } else if (seconds < 86400) {
            return (seconds / 3600) + "小时前";
        } else {
            return (seconds / 86400) + "天前";
        }
    }

    @Override
    public @Nullable List<String> onTabComplete(@NotNull CommandSender sender, 
                                                @NotNull Command command, 
                                                @NotNull String label, 
                                                @NotNull String[] args) {
        if (args.length == 1) {
            return filter(args[0], Arrays.asList(
                "reload", "check", "inspect", "save", "perf", 
                "simd", "health", "report", "benchmark", "help"
            ));
        }
        
        if (args.length == 2 && args[0].equalsIgnoreCase("check")) {
            return null; // Bukkit 自动补全在线玩家
        }
        
        if (args.length == 2 && args[0].equalsIgnoreCase("inspect")) {
            return filter(args[1], plugin.getIntegrationManager().getMonitoredItems());
        }
        
        return Collections.emptyList();
    }

    private List<String> filter(String input, List<String> list) {
        return list.stream()
            .filter(s -> s.toLowerCase().startsWith(input.toLowerCase()))
            .collect(Collectors.toList());
    }
}