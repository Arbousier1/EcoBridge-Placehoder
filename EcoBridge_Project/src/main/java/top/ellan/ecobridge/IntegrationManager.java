package top.ellan.ecobridge;

import cn.superiormc.ultimateshop.api.ShopHelper;
import cn.superiormc.ultimateshop.managers.ConfigManager;
import cn.superiormc.ultimateshop.objects.ObjectShop;
import cn.superiormc.ultimateshop.objects.buttons.ObjectItem;
import cn.superiormc.ultimateshop.objects.caches.ObjectUseTimesCache;
import cn.superiormc.ultimateshop.utils.ItemUtil;
import org.bukkit.Bukkit;
import org.bukkit.inventory.ItemStack; // 显式导入以确保类型清晰

import java.util.*;

/**
 * IntegrationManager - 核心集成管理器
 * <p>
 * 负责连接 UltimateShop 与 EcoBridge 的 PID 系统。
 * 提供高性能的数据采集、增量计算以及元数据预解析功能。
 * </p>
 */
public class IntegrationManager {

    /**
     * 交易元数据对象
     */
    public record TradeMetadata(
            String rawId,      // 唯一标识 (Shop_Product)
            String displayName,// 解析后的展示名称 (带颜色或纯文本)
            String shopTitle   // 所属商店标题 (此处使用 Shop ID)
    ) {}

    /**
     * 内部条目封装
     */
    private record ItemEntry(
            String rawId,       // 字符串 ID
            int pidHandle,      // PID 快速句柄
            ObjectItem item,    // UltimateShop 对象引用
            TradeMetadata metadata // 预解析的元数据
    ) {}

    private final EcoBridge plugin;

    // 活动条目列表 (Copy-On-Write 策略)
    private volatile List<ItemEntry> activeEntries = Collections.emptyList();

    // 指令补全专用缓存
    private volatile List<String> monitoredIdCache = Collections.emptyList();

    // 记录上一次的累积销量
    private final Map<Integer, Double> lastTotalVolumes = new HashMap<>(4096);

    public IntegrationManager(EcoBridge plugin) {
        this.plugin = plugin;
    }

    /**
     * 获取当前所有受控物品的元数据快照
     */
    public List<TradeMetadata> getRecentTradesMetadata() {
        final List<ItemEntry> current = this.activeEntries;
        List<TradeMetadata> list = new ArrayList<>(current.size());
        for (ItemEntry entry : current) {
            list.add(entry.metadata());
        }
        return list;
    }

    /**
     * 数据采集核心逻辑
     */
    public void collectDataAndCalculate() {
        final List<ItemEntry> currentItems = this.activeEntries;
        if (currentItems.isEmpty()) {
            if (Bukkit.getPluginManager().isPluginEnabled("UltimateShop")) {
                syncShops();
                if (activeEntries.isEmpty()) return;
            } else {
                return;
            }
        }

        final int size = currentItems.size();
        int[] handles = new int[size];
        double[] deltaVolumes = new double[size];
        int count = 0;

        for (int i = 0; i < size; i++) {
            ItemEntry entry = currentItems.get(i);

            try {
                ObjectUseTimesCache cache = ShopHelper.getServerUseTimesCache(entry.item());

                if (cache != null) {
                    // 计算总交易量 (买入 + 卖出)
                    double currentTotal = cache.getBuyUseTimes() + cache.getSellUseTimes();

                    // 计算增量
                    double lastTotal = lastTotalVolumes.getOrDefault(entry.pidHandle(), currentTotal);
                    double delta = currentTotal - lastTotal;

                    // 更新记录
                    lastTotalVolumes.put(entry.pidHandle(), currentTotal);

                    handles[count] = entry.pidHandle();
                    deltaVolumes[count] = delta;
                    count++;
                }
            } catch (Exception ignored) {
                // 容错处理
            }
        }

        if (count == 0) return;

        final int finalCount = count;
        final int[] finalHandles = (count == size) ? handles : Arrays.copyOf(handles, count);
        final double[] finalDeltas = (count == size) ? deltaVolumes : Arrays.copyOf(deltaVolumes, count);

        if (plugin.getPidController() != null) {
            plugin.getPidController().calculateBatch(finalHandles, finalDeltas, finalCount);
        }
    }

    /**
     * 同步商店与商品列表
     */
    public synchronized void syncShops() {
        if (!Bukkit.getPluginManager().isPluginEnabled("UltimateShop") ||
                ConfigManager.configManager == null ||
                ConfigManager.configManager.shopConfigs == null) {
            return;
        }

        Map<String, ObjectShop> shopMap = ConfigManager.configManager.shopConfigs;
        if (shopMap.isEmpty()) return;

        List<ItemEntry> newEntries = new ArrayList<>(shopMap.size() * 10);
        List<String> newIds = new ArrayList<>(shopMap.size() * 10);

        PidController pidCtrl = plugin.getPidController();
        if (pidCtrl == null) return;

        for (Map.Entry<String, ObjectShop> entry : shopMap.entrySet()) {
            String shopId = entry.getKey();
            ObjectShop shop = entry.getValue();
            if (shop == null) continue;

            List<ObjectItem> productList = shop.getProductList();
            if (productList == null) continue;

            // [修复 1] ObjectShop 通常没有 getShopTitle() 方法，直接使用 shopId 作为标题
            String shopTitle = shopId; 

            for (ObjectItem item : productList) {
                String productId = item.getProduct();
                if (productId == null || productId.isEmpty()) continue;

                String rawId = shopId + "_" + productId;
                int handle = pidCtrl.getHandle(rawId);

                String prettyName = item.getDisplayName(null);

                if (prettyName == null || prettyName.isEmpty()) {
                    try {
                        // [修复 2] getDisplayItem(null) 返回的就是 ItemStack，不需要再调 getItemStack()
                        ItemStack displayItem = item.getDisplayItem(null);
                        prettyName = ItemUtil.getItemName(displayItem);
                    } catch (Exception e) {
                        // 极致兜底
                        prettyName = rawId;
                    }
                }

                TradeMetadata metadata = new TradeMetadata(rawId, prettyName, shopTitle);

                newEntries.add(new ItemEntry(rawId, handle, item, metadata));
                newIds.add(rawId);

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