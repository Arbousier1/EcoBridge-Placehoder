package top.ellan.ecobridge;

import cn.superiormc.ultimateshop.api.ShopHelper;
import cn.superiormc.ultimateshop.managers.ConfigManager;
import cn.superiormc.ultimateshop.objects.ObjectShop;
import cn.superiormc.ultimateshop.objects.buttons.ObjectItem;
import cn.superiormc.ultimateshop.objects.caches.ObjectUseTimesCache;
import org.bukkit.Bukkit;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class IntegrationManager {
    // 缓存元数据记录
    public record ItemMeta(String shopId, String productId) {}

    private final EcoBridge plugin;
    // 解析缓存：rawId -> Meta
    private final ConcurrentHashMap<String, ItemMeta> idParserCache = new ConcurrentHashMap<>();
    // 监控列表 (线程安全)
    private final List<String> monitoredItems = Collections.synchronizedList(new ArrayList<>());

    public IntegrationManager(EcoBridge plugin) {
        this.plugin = plugin;
    }

    /**
     * 数据采集核心逻辑
     * 必须在 主线程 (Sync) 运行，因为 UltimateShop API 读取非线程安全
     */
    public void collectDataAndCalculate() {
        // 如果列表为空，尝试同步一次商店配置
        if (monitoredItems.isEmpty()) {
            syncShops();
        }

        if (monitoredItems.isEmpty()) return;

        // --- SIMD 批量数据准备 ---
        // 使用 ArrayList 保证顺序，方便 SIMD 转换
        List<String> activeIds = new ArrayList<>(monitoredItems.size());
        
        // 原始数组用于 SIMD 计算，预分配大小
        // double[] 的内存布局是连续的，非常适合 Vector API 加载
        double[] volumes = new double[monitoredItems.size()];
        int idx = 0;

        for (String rawId : monitoredItems) {
            ItemMeta meta = idParserCache.get(rawId);
            if (meta == null) continue;

            try {
                // 1. 通过 ShopID 和 ProductID 获取商品对象
                ObjectItem item = ShopHelper.getItemFromID(meta.shopId(), meta.productId());
                
                if (item != null) {
                    // 2. 获取全服交易次数缓存 (买入次数 - 卖出次数 = 净销量)
                    ObjectUseTimesCache cache = ShopHelper.getServerUseTimesCache(item);
                    
                    double netVolume = 0.0;
                    if (cache != null) {
                        double buy = cache.getBuyUseTimes();
                        double sell = cache.getSellUseTimes();
                        netVolume = buy - sell;
                    }
                    
                    // 将有效数据加入批量容器
                    activeIds.add(rawId);
                    volumes[idx++] = netVolume;
                }
            } catch (Exception e) {
                // 容错处理 (生产环境可降低日志级别)
                // plugin.getLogger().warning("Error collecting data for " + rawId + ": " + e.getMessage());
            }
        }

        // 如果没有有效数据，直接返回
        if (idx == 0) return;

        // 截断数组 (如果有无效物品导致 idx < size)
        final int finalCount = idx;
        final List<String> finalIds = activeIds;
        final double[] finalVolumes = Arrays.copyOf(volumes, finalCount); 

        // 3. 提交批量任务到异步线程池进行 SIMD 计算
        // 虽然 Vector API 极快，但依然建议移出主线程以防万一
        Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
            plugin.getPidController().calculateBatch(finalIds, finalVolumes);
        });
    }

    /**
     * 同步商店列表
     * 遍历 UltimateShop 的配置，将所有商品加入监控列表
     */
    public void syncShops() {
        // 确保 ConfigManager 已加载
        if (ConfigManager.configManager == null || ConfigManager.configManager.shopConfigs == null) {
            plugin.getLogger().warning("UltimateShop ConfigManager not initialized yet.");
            return;
        }

        // 获取所有商店配置 Map<ShopID, ObjectShop>
        Map<String, ObjectShop> shopMap = ConfigManager.configManager.shopConfigs;
        
        if (shopMap.isEmpty()) return;

        monitoredItems.clear();
        idParserCache.clear();

        for (Map.Entry<String, ObjectShop> entry : shopMap.entrySet()) {
            String shopId = entry.getKey();
            ObjectShop shop = entry.getValue();
            
            if (shop == null) continue;

            // 获取该商店内的所有商品
            List<ObjectItem> productList = shop.getProductList();
            if (productList == null) continue;

            for (ObjectItem item : productList) {
                String productId = item.getProduct(); // 获取商品ID
                
                if (productId == null || productId.isEmpty()) continue;

                // 生成唯一标识符 rawId (格式: shopId_productId)
                String rawId = shopId + "_" + productId;

                // 存入解析缓存，供采集时快速查找
                idParserCache.put(rawId, new ItemMeta(shopId, productId));
                monitoredItems.add(rawId);
            }
        }
        
        plugin.getLogger().info("Synced " + monitoredItems.size() + " items from UltimateShop for SIMD processing.");
    }
}