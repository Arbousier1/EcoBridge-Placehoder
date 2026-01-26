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
 * 优化重点：高性能对象化索引、规避 API 查找损耗、线程安全读取
 */
public class IntegrationManager {

    /**
     * 内部条目：缓存所有高频读取的元数据 [cite: 71, 80]
     */
    private record ItemEntry(
            String rawId,       // PID 控制器唯一识别码 (shopId_productId)
            ObjectItem item     // 持有 UltimateShop 商品对象引用，规避重复查找 [cite: 72]
    ) {}

    private final EcoBridge plugin;
    
    // 活动条目列表：采集循环的核心数据源
    private List<ItemEntry> activeEntries = new ArrayList<>();
    
    // 指令补全专用缓存：使用 volatile 保证异步读取的可见性 [cite: 58, 69]
    private volatile List<String> monitoredIdCache = Collections.emptyList();

    public IntegrationManager(EcoBridge plugin) {
        this.plugin = plugin;
    }

    /**
     * 数据采集核心逻辑
     * 运行在主线程 (Sync)，提取销量数据并投递至异步 SIMD 引擎 [cite: 27, 76]
     */
    public void collectDataAndCalculate() {
        // 若监控列表为空，尝试自动同步一次
        if (activeEntries.isEmpty()) {
            syncShops();
            if (activeEntries.isEmpty()) return;
        }

        // 使用局部变量引用，确保单次循环的数据一致性
        final List<ItemEntry> currentItems = this.activeEntries;
        final int size = currentItems.size();

        // 预分配容器，存储待计算的 ID 和 净销量 [cite: 71]
        List<String> ids = new ArrayList<>(size);
        double[] volumes = new double[size];
        int count = 0;

        // --- 极速采集循环 ---
        for (int i = 0; i < size; i++) {
            ItemEntry entry = currentItems.get(i);
            
            try {
                // 直接通过持有对象获取全服交易次数 [cite: 72, 73]
                ObjectUseTimesCache cache = ShopHelper.getServerUseTimesCache(entry.item());
                
                if (cache != null) {
                    // 净销量 = 买入 - 卖出 [cite: 74]
                    double netVolume = cache.getBuyUseTimes() - cache.getSellUseTimes();
                    
                    ids.add(entry.rawId());
                    volumes[count++] = netVolume;
                }
            } catch (Exception ignored) {
                // 静默处理单个商品采集错误，维持整体运行
            }
        }

        if (count == 0) return;

        // 精准截断数据，确保传给 PID 控制器的数组没有空尾部 [cite: 75]
        final List<String> finalIds = ids;
        final double[] finalVolumes = (count == size) ? volumes : Arrays.copyOf(volumes, count);

        // 异步执行 SIMD 向量化计算，释放主线程 [cite: 76, 130]
        Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
            plugin.getPidController().calculateBatch(finalIds, finalVolumes);
        });
    }

    /**
     * 同步商店与商品列表
     * 遍历 UltimateShop 配置，构建 EcoBridge 内部高效索引 [cite: 77-80]
     */
    public synchronized void syncShops() {
        if (ConfigManager.configManager == null || ConfigManager.configManager.shopConfigs == null) {
            plugin.getLogger().warning("UltimateShop 核心尚未加载完毕，同步延迟。 [cite: 76]");
            return;
        }

        Map<String, ObjectShop> shopMap = ConfigManager.configManager.shopConfigs;
        if (shopMap.isEmpty()) return;

        // 预估容量：商店数 * 平均 10 个物品 [cite: 78, 79]
        List<ItemEntry> newEntries = new ArrayList<>(shopMap.size() * 10);
        List<String> newIds = new ArrayList<>(shopMap.size() * 10);

        for (Map.Entry<String, ObjectShop> entry : shopMap.entrySet()) {
            String shopId = entry.getKey();
            ObjectShop shop = entry.getValue();
            if (shop == null) continue;

            List<ObjectItem> productList = shop.getProductList();
            if (productList == null) continue;

            for (ObjectItem item : productList) {
                String productId = item.getProduct(); // 获取配置键名 [cite: 79]
                if (productId == null || productId.isEmpty()) continue;

                // 提前拼接唯一识别码，避免运行时计算 [cite: 80]
                String rawId = shopId + "_" + productId;
                
                newEntries.add(new ItemEntry(rawId, item));
                newIds.add(rawId);
            }
        }

        // 原子替换引用，旧列表交由 GC 处理 
        this.activeEntries = newEntries;
        this.monitoredIdCache = Collections.unmodifiableList(newIds);

        plugin.getLogger().info("成功同步 " + activeEntries.size() + " 个监控物品。 ");
    }

    /**
     * 获取受监控 ID 缓存
     * 用于 EcoBridgeCommand 的 TabCompletion 零延迟补全 [cite: 57, 58]
     */
    public List<String> getMonitoredItems() {
        return monitoredIdCache;
    }
}