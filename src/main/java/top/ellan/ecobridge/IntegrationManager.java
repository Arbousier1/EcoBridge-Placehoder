package top.ellan.ecobridge;

import cn.superiormc.ultimateshop.api.ShopHelper;
import cn.superiormc.ultimateshop.managers.ConfigManager;
import cn.superiormc.ultimateshop.objects.ObjectShop;
import cn.superiormc.ultimateshop.objects.buttons.ObjectItem;
import cn.superiormc.ultimateshop.objects.caches.ObjectUseTimesCache;
import org.bukkit.Bukkit;

import java.util.*;

/**
 * 集成管理器 - 极致性能最终版 (已修正：使用实际交易数量)
 * 
 * 特性：Direct Memory Access, Array-based Indexing, Snapshot Architecture
 * 
 * ========================================================================
 * 【2024修正版】核心改进：
 *   - 使用 TransactionAmountCache 获取实际交易数量（而非次数）
 *   - 公式：actualAmount = multi × amountPlaceholder
 *   - 卖出 2304 个物品 → delta = 2304（而非旧的 delta = 1）
 *   - 现在可以正确触发价格腰斩效果！
 * ========================================================================
 */
public class IntegrationManager {

    /**
     * 极简 Entry，只存 Hot Path 必须的直接内存引用
     */
    private record FastItemEntry(
            int pidHandle,                  // 数组索引句柄 (用于 lastTotalVolumes[handle])
            String itemId,                  // 物品ID (用于从 TransactionAmountCache 获取数量)
            ObjectUseTimesCache dataCache   // UltimateShop 数据对象直接引用 (保留用于备用)
    ) {}

    private final EcoBridge plugin;

    // ==================== 运行时热数据 (Hot Data) ====================
    private volatile FastItemEntry[] fastEntries = new FastItemEntry[0];

    /**
     * 记录上一 Tick 的净总量
     * Index = Handle
     * Value = NetVolume (Sell - Buy) 的实际物品数量
     */
    private double[] lastTotalVolumes = new double[0];

    // ==================== 辅助数据 (Cold Data) ====================
    private volatile List<String> monitoredIdCache = Collections.emptyList();

    public IntegrationManager(EcoBridge plugin) {
        this.plugin = plugin;
    }

    /**
     * [Phase 1: Data Capture]
     * 运行在主线程 (Sync)，耗时必须控制在微秒级
     * 
     * 【核心改进】现在从 TransactionAmountCache 获取实际交易数量
     * 
     * @return MarketSnapshot 包含本周期所有物品的交易增量
     */
    public EcoBridge.MarketSnapshot captureDataSnapshot() {
        final FastItemEntry[] entries = this.fastEntries;
        final int size = entries.length;

        if (size == 0) {
            if (Bukkit.getPluginManager().isPluginEnabled("UltimateShop")) {
                syncShops();
                if (fastEntries.length == 0) return null;
            }
            return null;
        }

        int[] snapshotHandles = new int[size];
        double[] snapshotDeltas = new double[size];
        int count = 0;

        // 获取交易数量缓存
        TransactionAmountCache amountCache = TransactionAmountCache.getInstance();

        for (int i = 0; i < size; i++) {
            FastItemEntry entry = entries[i];
            
            if (entry == null) continue;

            try {
                // ====================================================================
                // 【关键改进】从 TransactionAmountCache 获取实际交易数量
                // 
                // 市场压力方向（取负值）：
                //   - 玩家卖出物品 → rawDelta > 0 (sell 增加)
                //     → 供应增加 → 价格应下降 → delta 为负
                //   - 玩家买入物品 → rawDelta < 0 (buy 增加)  
                //     → 需求增加 → 价格应上涨 → delta 为正
                // ====================================================================
                
                double currentTotal = 0;
                
                // 从缓存获取实际数量
                TransactionAmountCache.AmountCounter counter = amountCache.get(entry.itemId);
                if (counter != null) {
                    // 净流量 = 卖出数量 - 买入数量
                    currentTotal = counter.getNetFlow();
                }

                int handle = entry.pidHandle;
                double lastTotal = lastTotalVolumes[handle];
                
                // 计算原始增量
                double rawDelta = currentTotal - lastTotal;
                
                // 【关键】取负值，转换为市场压力方向
                // 卖出压力（rawDelta > 0）→ 价格下降 → delta 为负
                // 买入需求（rawDelta < 0）→ 价格上涨 → delta 为正
                double delta = -rawDelta;

                // Write Back 更新历史状态
                lastTotalVolumes[handle] = currentTotal;

                // 录入快照
                snapshotHandles[count] = handle;
                snapshotDeltas[count] = delta;
                count++;

            } catch (Exception ignored) {
            }
        }

        if (count == 0) return null;

        double totalBalance = 0.0;
        int onlinePlayers = Bukkit.getOnlinePlayers().size();

        return new EcoBridge.MarketSnapshot(
            totalBalance,
            onlinePlayers,
            System.currentTimeMillis(),
            snapshotHandles, 
            snapshotDeltas,
            count
        );
    }

    /**
     * 同步商店数据
     */
    public synchronized void syncShops() {
        if (!Bukkit.getPluginManager().isPluginEnabled("UltimateShop")) {
            plugin.getLogger().warning("[Integration] UltimateShop not enabled, skipping sync");
            return;
        }
        
        if (ConfigManager.configManager == null || ConfigManager.configManager.shopConfigs == null) {
            plugin.getLogger().warning("[Integration] UltimateShop ConfigManager not ready");
            return;
        }

        Map<String, ObjectShop> shopMap = ConfigManager.configManager.shopConfigs;
        if (shopMap.isEmpty()) {
            plugin.getLogger().warning("[Integration] No shops found in UltimateShop");
            return;
        }

        List<FastItemEntry> tempEntries = new ArrayList<>(shopMap.size() * 5);
        List<String> tempIds = new ArrayList<>();
        
        PidController pidCtrl = plugin.getPidController();
        if (pidCtrl == null) {
            plugin.getLogger().severe("[Integration] PID Controller not initialized!");
            return;
        }

        int maxHandle = 0;

        for (Map.Entry<String, ObjectShop> entry : shopMap.entrySet()) {
            String shopId = entry.getKey();
            ObjectShop shop = entry.getValue();
            
            if (shop == null || shop.getProductList() == null) continue;

            for (ObjectItem item : shop.getProductList()) {
                String productId = item.getProduct();
                if (productId == null || productId.isEmpty()) continue;

                String rawId = shopId + "_" + productId;
                int handle = pidCtrl.getHandle(rawId);
                
                if (handle > maxHandle) maxHandle = handle;

                ObjectUseTimesCache cache = ShopHelper.getServerUseTimesCache(item);
                
                if (cache != null) {
                    // 【改进】现在保存 itemId，用于从 TransactionAmountCache 获取数量
                    tempEntries.add(new FastItemEntry(handle, rawId, cache));
                    tempIds.add(rawId);
                } else {
                    plugin.getLogger().warning("[Integration] Failed to get cache for: " + rawId);
                }
            }
        }

        if (lastTotalVolumes.length <= maxHandle) {
            double[] newVolumes = new double[maxHandle + 256];
            System.arraycopy(
                lastTotalVolumes, 0, 
                newVolumes, 0, 
                Math.min(lastTotalVolumes.length, newVolumes.length)
            );
            this.lastTotalVolumes = newVolumes;
        }

        // 【改进】初始化时从缓存获取当前数量
        TransactionAmountCache amountCache = TransactionAmountCache.getInstance();
        
        for (FastItemEntry entry : tempEntries) {
            double current = 0;
            TransactionAmountCache.AmountCounter counter = amountCache.get(entry.itemId);
            if (counter != null) {
                current = counter.getNetFlow();
            }
            
            if (lastTotalVolumes[entry.pidHandle] == 0) {
                lastTotalVolumes[entry.pidHandle] = current;
            }
        }

        this.fastEntries = tempEntries.toArray(new FastItemEntry[0]);
        this.monitoredIdCache = Collections.unmodifiableList(tempIds);

        plugin.getLogger().info(String.format(
            "[Integration] Optimized Sync Complete: %d items linked (Max Handle: %d) [Using Actual Amounts]",
            fastEntries.length, maxHandle
        ));
    }

    /**
     * 获取当前监控的所有物品 ID 列表
     */
    public List<String> getMonitoredItems() {
        return monitoredIdCache;
    }
}
