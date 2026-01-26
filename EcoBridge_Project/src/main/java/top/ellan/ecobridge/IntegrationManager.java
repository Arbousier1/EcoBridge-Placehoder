package top.ellan.ecobridge;

import cn.superiormc.ultimateshop.api.ShopHelper;
import cn.superiormc.ultimateshop.managers.ConfigManager;
import cn.superiormc.ultimateshop.objects.ObjectShop;
import cn.superiormc.ultimateshop.objects.buttons.ObjectItem;
import cn.superiormc.ultimateshop.objects.caches.ObjectUseTimesCache;
import org.bukkit.Bukkit;

import java.util.*;

/**
 * 集成管理器 - 负责与 UltimateShop 数据对接
 * 
 * 修复内容:
 * 1. 适配 OptimizedPidController 的 Handle (int) 机制
 * 2. 增加 Delta 计算 (当前销量 - 上次销量)，确保 PID 获取的是流量数据
 * 3. 零 GC 采集循环
 */
public class IntegrationManager {

    /**
     * 内部条目：缓存 PID 句柄，避免运行时 Map 查找
     */
    private record ItemEntry(
            String rawId,       // 字符串 ID (用于指令补全)
            int pidHandle,      // PID 快速句柄 (核心优化)
            ObjectItem item     // UltimateShop 对象引用
    ) {}

    private final EcoBridge plugin;
    
    // 活动条目列表 (Copy-On-Write 策略)
    private volatile List<ItemEntry> activeEntries = Collections.emptyList();
    
    // 指令补全专用缓存
    private volatile List<String> monitoredIdCache = Collections.emptyList();
    
    // 记录上一次的累积销量：Handle -> LastTotalVolume
    // 必须在主线程访问，或者加锁。由于 collectDataAndCalculate 在主线程运行，这里使用普通 Map 即可。
    // 为了极致性能，推荐使用 fastutil 的 Int2DoubleOpenHashMap，这里用原生数组模拟或 Map
    private final Map<Integer, Double> lastTotalVolumes = new HashMap<>(4096);

    public IntegrationManager(EcoBridge plugin) {
        this.plugin = plugin;
    }

    /**
     * 数据采集核心逻辑
     * 运行在主线程 (Sync) -> 提取增量 -> 投递给异步 PID
     */
    public void collectDataAndCalculate() {
        // 引用快照，保证线程安全
        final List<ItemEntry> currentItems = this.activeEntries;
        if (currentItems.isEmpty()) {
            // 尝试惰性初始化
            if (Bukkit.getPluginManager().isPluginEnabled("UltimateShop")) {
                syncShops();
                if (activeEntries.isEmpty()) return;
            } else {
                return;
            }
        }

        final int size = currentItems.size();
        
        // 使用原生数组，适配 OptimizedPidController 的 API
        int[] handles = new int[size];
        double[] deltaVolumes = new double[size];
        int count = 0;

        // --- 极速采集循环 ---
        for (int i = 0; i < size; i++) {
            ItemEntry entry = currentItems.get(i);
            
            try {
                // 1. 获取全服历史总销量 (Total Accumulation)
                ObjectUseTimesCache cache = ShopHelper.getServerUseTimesCache(entry.item());
                
                if (cache != null) {
                    // 净历史总量 = 买入 - 卖出 (根据需求调整，通常我们监控买入量)
                    // 如果要监控玩家卖给商店，那就是 SellUseTimes
                    // 这里假设监控商店热度：买入 + 卖出，或者仅买入
                    double currentTotal = cache.getBuyUseTimes() + cache.getSellUseTimes();
                    
                    // 2. 计算增量 (Delta) = 当前总量 - 上次记录的总量
                    // PID 控制器需要的是"这一段时间内发生了多少交易"
                    double lastTotal = lastTotalVolumes.getOrDefault(entry.pidHandle(), currentTotal);
                    double delta = currentTotal - lastTotal;
                    
                    // 3. 更新历史记录
                    lastTotalVolumes.put(entry.pidHandle(), currentTotal);
                    
                    // 4. 只有当数据有效时才录入 (或者即使是0也录入，取决于PID是否需要连续tick)
                    // PID 需要连续的时间流，即使是 0 也要传
                    handles[count] = entry.pidHandle();
                    deltaVolumes[count] = delta;
                    count++;
                }
            } catch (Exception ignored) {
                // 容错处理
            }
        }

        if (count == 0) return;

        // 截断数组以匹配实际数量 (如果发生异常跳过了一些)
        final int finalCount = count;
        final int[] finalHandles = (count == size) ? handles : Arrays.copyOf(handles, count);
        final double[] finalDeltas = (count == size) ? deltaVolumes : Arrays.copyOf(deltaVolumes, count);

        // 异步投递给 PID 控制器 (Non-Blocking)
        // 这里的 calculateBatch 必须对应 OptimizedPidController 的方法
        if (plugin.getPidController() != null) {
            plugin.getPidController().calculateBatch(finalHandles, finalDeltas, finalCount);
        }
    }

    /**
     * 同步商店与商品列表
     * 构建 Handle 索引
     */
    public synchronized void syncShops() {
        // 检查 UltimateShop 是否就绪
        if (!Bukkit.getPluginManager().isPluginEnabled("UltimateShop") || 
            ConfigManager.configManager == null || 
            ConfigManager.configManager.shopConfigs == null) {
            return;
        }

        Map<String, ObjectShop> shopMap = ConfigManager.configManager.shopConfigs;
        if (shopMap.isEmpty()) return;

        List<ItemEntry> newEntries = new ArrayList<>(shopMap.size() * 10);
        List<String> newIds = new ArrayList<>(shopMap.size() * 10);
        
        // 获取 PID 控制器引用
        PidController pidCtrl = plugin.getPidController();
        if (pidCtrl == null) return;

        for (Map.Entry<String, ObjectShop> entry : shopMap.entrySet()) {
            String shopId = entry.getKey();
            ObjectShop shop = entry.getValue();
            if (shop == null) continue;

            List<ObjectItem> productList = shop.getProductList();
            if (productList == null) continue;

            for (ObjectItem item : productList) {
                String productId = item.getProduct(); 
                if (productId == null || productId.isEmpty()) continue;

                // 构造唯一 ID
                String rawId = shopId + "_" + productId;
                
                // [关键修复] 在同步时直接获取/注册 PID Handle
                // 这样在采集循环中就不需要查 Map 了
                int handle = pidCtrl.getHandle(rawId);
                
                newEntries.add(new ItemEntry(rawId, handle, item));
                newIds.add(rawId);
                
                // 初始化该物品的 lastTotal，防止刚启动时产生巨大的 Delta 脉冲
                ObjectUseTimesCache cache = ShopHelper.getServerUseTimesCache(item);
                if (cache != null) {
                    double currentTotal = cache.getBuyUseTimes() + cache.getSellUseTimes();
                    lastTotalVolumes.put(handle, currentTotal);
                }
            }
        }

        this.activeEntries = newEntries;
        this.monitoredIdCache = Collections.unmodifiableList(newIds);

        plugin.getLogger().info("[Integration] Synced " + activeEntries.size() + " items from UltimateShop.");
    }

    public List<String> getMonitoredItems() {
        return monitoredIdCache;
    }
}