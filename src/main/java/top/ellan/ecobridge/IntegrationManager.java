package top.ellan.ecobridge;

import cn.superiormc.ultimateshop.api.ShopHelper;
import cn.superiormc.ultimateshop.managers.ConfigManager;
import cn.superiormc.ultimateshop.objects.ObjectShop;
import cn.superiormc.ultimateshop.objects.buttons.ObjectItem;
import cn.superiormc.ultimateshop.objects.caches.ObjectUseTimesCache;
import org.bukkit.Bukkit;

import java.util.*;

/**
 * 集成管理器 - 极致性能最终版 (已修正买卖逻辑 - 正确版)
 * 特性：Direct Memory Access, Array-based Indexing, Snapshot Architecture
 * 
 * ========================================================================
 * 逻辑修正（2024版）：
 *   - 使用 (卖出 - 买入) 计算净流量，实现供需平衡
 *   - 玩家卖出物品 → sell 增加 → delta 为正 → lambda 为正 → 价格下降 ✅
 *   - 玩家买入物品 → buy 增加 → delta 为负 → lambda 为负 → 价格上涨 ✅
 * ========================================================================
 */
public class IntegrationManager {

    /**
     * 极简 Entry，只存 Hot Path 必须的直接内存引用
     */
    private record FastItemEntry(
            int pidHandle,                  // 数组索引句柄 (用于 lastTotalVolumes[handle])
            ObjectUseTimesCache dataCache   // UltimateShop 数据对象直接引用 (跳过查找)
    ) {}

    private final EcoBridge plugin;

    // ==================== 运行时热数据 (Hot Data) ====================
    // 原生数组，CPU 缓存友好，无 List overhead
    private volatile FastItemEntry[] fastEntries = new FastItemEntry[0];

    /**
     * 记录上一 Tick 的净总量
     * Index = Handle
     * Value = NetVolume (Sell - Buy)
     * 
     * 扩容策略：在 syncShops 中按最大 Handle ID + Padding 分配
     */
    private double[] lastTotalVolumes = new double[0];

    // ==================== 辅助数据 (Cold Data) ====================
    // 仅用于指令补全和监控
    private volatile List<String> monitoredIdCache = Collections.emptyList();

    public IntegrationManager(EcoBridge plugin) {
        this.plugin = plugin;
    }

    /**
     * [Phase 1: Data Capture]
     * 运行在主线程 (Sync)，耗时必须控制在微秒级
     * 返回不可变快照供 Phase 2 (Async) 使用
     * 
     * @return MarketSnapshot 包含本周期所有物品的交易增量
     */
    public EcoBridge.MarketSnapshot captureDataSnapshot() {
        // 1. 获取本地引用，保证并发安全 (Copy-On-Write 读侧)
        final FastItemEntry[] entries = this.fastEntries;
        final int size = entries.length;

        if (size == 0) {
            // 惰性初始化尝试 (通常只在第一次或重载后发生)
            if (Bukkit.getPluginManager().isPluginEnabled("UltimateShop")) {
                syncShops();
                // 如果初始化后还是空，本 tick 先跳过
                if (fastEntries.length == 0) return null;
            }
            return null;
        }

        // 2. 预分配快照数组 (Java 小数组分配极快，Eden 区分配)
        int[] snapshotHandles = new int[size];
        double[] snapshotDeltas = new double[size];
        int count = 0;

        // 3. 极速循环 (Hot Loop)
        for (int i = 0; i < size; i++) {
            FastItemEntry entry = entries[i];
            
            // 安全检查：防止 UltimateShop 重载导致对象失效
            if (entry == null || entry.dataCache == null) continue;

            try {
                // ====================================================================
                // [关键修改] 使用减法计算净流量 (Net Flow)
                // 公式：卖出 - 买入 (Sell - Buy)
                // 
                // 逻辑说明：
                //   - 玩家向商店卖出物品 → sellUseTimes 增加
                //     → currentTotal 增加 → delta 为正
                //     → lambda 为正 → 价格下降（供应增加） ✅
                // 
                //   - 玩家从商店买入物品 → buyUseTimes 增加
                //     → currentTotal 减少 → delta 为负
                //     → lambda 为负 → 价格上涨（需求增加） ✅
                // ====================================================================
                double currentTotal = entry.dataCache.getSellUseTimes() 
                                    - entry.dataCache.getBuyUseTimes();
                
                int handle = entry.pidHandle;

                // [优化] 数组直接寻址 O(1)，无 Map 哈希开销
                double lastTotal = lastTotalVolumes[handle];
                
                // 计算增量 (本 tick 的净变化量)
                // 示例1：上次净卖出100，现在净卖出105 → delta = +5 (卖出压力增加，降价)
                // 示例2：上次净卖出100，现在净卖出95 → delta = -5 (有人买入，涨价)
                double delta = currentTotal - lastTotal;

                // Write Back 更新历史状态
                lastTotalVolumes[handle] = currentTotal;

                // 录入快照
                snapshotHandles[count] = handle;
                snapshotDeltas[count] = delta;
                count++;

            } catch (Exception ignored) {
                // 忽略极少数并发异常，保护主线程
                // 可能原因：UltimateShop 在热重载过程中修改了数据结构
            }
        }

        // 4. 如果本周期没有任何有效数据，返回 null
        if (count == 0) return null;

        // 5. 获取全局经济数据 (如果需要，例如 Vault 总余额，可以在此 hook)
        // 这里仅为示例预留字段
        double totalBalance = 0.0; 
        int onlinePlayers = Bukkit.getOnlinePlayers().size();

        // 6. 返回不可变快照 (Record 是不可变的，且传递给 Virtual Thread 安全)
        return new EcoBridge.MarketSnapshot(
            totalBalance,
            onlinePlayers,
            System.currentTimeMillis(),
            snapshotHandles, 
            snapshotDeltas,
            count // 实际有效数量
        );
    }

    /**
     * 同步商店数据
     * 该方法较重，但只在启动或重载时运行
     * 
     * 执行内容：
     *   1. 扫描所有 UltimateShop 商店配置
     *   2. 为每个商品注册 PID 控制器句柄
     *   3. 构建快速访问索引（FastItemEntry 数组）
     *   4. 初始化历史状态，防止启动时的价格脉冲
     */
    public synchronized void syncShops() {
        // 前置检查：UltimateShop 是否可用
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

        // 预分配临时容器
        List<FastItemEntry> tempEntries = new ArrayList<>(shopMap.size() * 5);
        List<String> tempIds = new ArrayList<>();
        
        PidController pidCtrl = plugin.getPidController();
        if (pidCtrl == null) {
            plugin.getLogger().severe("[Integration] PID Controller not initialized!");
            return;
        }

        int maxHandle = 0;

        // ========================================================================
        // Phase 1: 遍历并构建索引
        // ========================================================================
        for (Map.Entry<String, ObjectShop> entry : shopMap.entrySet()) {
            String shopId = entry.getKey();
            ObjectShop shop = entry.getValue();
            
            if (shop == null || shop.getProductList() == null) continue;

            for (ObjectItem item : shop.getProductList()) {
                String productId = item.getProduct();
                if (productId == null || productId.isEmpty()) continue;

                // 构造全局唯一 ID：shopId_productId
                // 示例：main_diamond, vip_golden_apple
                String rawId = shopId + "_" + productId;
                
                // 注册到 PID 控制器，获取数组索引句柄
                int handle = pidCtrl.getHandle(rawId);
                
                // 记录最大 Handle 以便分配数组
                if (handle > maxHandle) maxHandle = handle;

                // [核心优化] 预先获取 UltimateShop 的缓存对象引用 (Direct Pointer)
                // 跳过每次查找 Map 的开销
                ObjectUseTimesCache cache = ShopHelper.getServerUseTimesCache(item);
                
                if (cache != null) {
                    tempEntries.add(new FastItemEntry(handle, cache));
                    tempIds.add(rawId);
                } else {
                    plugin.getLogger().warning("[Integration] Failed to get cache for: " + rawId);
                }
            }
        }

        // ========================================================================
        // Phase 2: 调整原生数组大小 (LastTotalVolumes)
        // ========================================================================
        if (lastTotalVolumes.length <= maxHandle) {
            // 扩容策略：当前需求 + 256 缓冲（防止频繁扩容）
            double[] newVolumes = new double[maxHandle + 256];
            
            // 复制旧数据，防止状态丢失导致的瞬间 Delta 脉冲
            // 示例：物品原来有历史数据 100，扩容后还是 100，不会突然变 0
            System.arraycopy(
                lastTotalVolumes, 0, 
                newVolumes, 0, 
                Math.min(lastTotalVolumes.length, newVolumes.length)
            );
            
            this.lastTotalVolumes = newVolumes;
        }

        // ========================================================================
        // Phase 3: 初始化新加入物品的 LastTotal
        // ========================================================================
        // 防止场景：
        //   - 新物品加入时，当前 sellUseTimes = 1000, buyUseTimes = 500
        //   - currentTotal = 500, lastTotal = 0
        //   - delta = 500 → 瞬间触发巨大价格调整
        // 
        // 解决方案：
        //   - 首次加载时，将 currentTotal 写入 lastTotal
        //   - 这样 delta = 0，价格保持稳定
        // ========================================================================
        for (FastItemEntry entry : tempEntries) {
            // [关键修改] 这里也必须使用 sell - buy，保持与 captureDataSnapshot 一致
            double current = entry.dataCache.getSellUseTimes() 
                           - entry.dataCache.getBuyUseTimes();
            
            // 只有当该 Handle 的历史数据为 0 时才初始化
            // (说明是新物品或者是第一次加载)
            if (lastTotalVolumes[entry.pidHandle] == 0) {
                lastTotalVolumes[entry.pidHandle] = current;
            }
        }

        // ========================================================================
        // Phase 4: 原子性替换引用 (Copy-On-Write 发布)
        // ========================================================================
        this.fastEntries = tempEntries.toArray(new FastItemEntry[0]);
        this.monitoredIdCache = Collections.unmodifiableList(tempIds);

        plugin.getLogger().info(String.format(
            "[Integration] Optimized Sync Complete: %d items linked (Max Handle: %d)",
            fastEntries.length, maxHandle
        ));
    }

    /**
     * 获取当前监控的所有物品 ID 列表
     * 用于命令补全和调试
     * 
     * @return 不可变的物品 ID 列表
     */
    public List<String> getMonitoredItems() {
        return monitoredIdCache;
    }
}
