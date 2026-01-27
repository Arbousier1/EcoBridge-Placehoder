package top.ellan.ecobridge;

import cn.superiormc.ultimateshop.api.ShopHelper;
import cn.superiormc.ultimateshop.managers.ConfigManager;
import cn.superiormc.ultimateshop.objects.ObjectShop;
import cn.superiormc.ultimateshop.objects.buttons.ObjectItem;
import cn.superiormc.ultimateshop.objects.caches.ObjectUseTimesCache;
import org.bukkit.Bukkit;

import java.util.*;

/**
 * 集成管理器 - 极致性能最终版
 * 特性：Direct Memory Access, Array-based Indexing, Snapshot Architecture
 */
public class IntegrationManager {

    // 极简 Entry，只存 Hot Path 必须的直接内存引用
    private record FastItemEntry(
            int pidHandle,                  // 数组索引句柄 (用于 lastTotalVolumes[handle])
            ObjectUseTimesCache dataCache   // UltimateShop 数据对象直接引用 (跳过查找)
    ) {}

    private final EcoBridge plugin;

    // ==================== 运行时热数据 (Hot Data) ====================
    // 原生数组，CPU 缓存友好，无 List overhead
    private volatile FastItemEntry[] fastEntries = new FastItemEntry[0];

    // 记录上一 Tick 的总量：Index = Handle, Value = TotalVolume
    // 扩容策略：在 syncShops 中按最大 Handle ID + Padding 分配
    private double[] lastTotalVolumes = new double[0];

    // ==================== 辅助数据 (Cold Data) ====================
    // 仅用于指令补全
    private volatile List<String> monitoredIdCache = Collections.emptyList();

    public IntegrationManager(EcoBridge plugin) {
        this.plugin = plugin;
    }

    /**
     * [Phase 1: Data Capture]
     * 运行在主线程 (Sync)，耗时必须控制在微秒级
     * 返回不可变快照供 Phase 2 (Async) 使用
     */
    public EcoBridge.MarketSnapshot captureDataSnapshot() {
        // 1. 获取本地引用，保证并发安全 (Copy-On-Write 读侧)
        final FastItemEntry[] entries = this.fastEntries;
        final int size = entries.length;

        if (size == 0) {
            // 惰性初始化尝试 (通常只在第一次或重载后发生)
            if (Bukkit.getPluginManager().isPluginEnabled("UltimateShop")) {
                syncShops();
                // 如果初始化后还是空，或者这 tick 先跳过
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
                // [优化] 直接读取内存值，无方法调用开销
                double currentTotal = entry.dataCache.getBuyUseTimes() + entry.dataCache.getSellUseTimes();
                int handle = entry.pidHandle;

                // [优化] 数组直接寻址 O(1)，无 Map 哈希开销
                double lastTotal = lastTotalVolumes[handle];
                
                // 计算增量 (本 tick 交易量)
                double delta = currentTotal - lastTotal;

                // Write Back 更新历史状态
                lastTotalVolumes[handle] = currentTotal;

                // 录入快照
                snapshotHandles[count] = handle;
                snapshotDeltas[count] = delta;
                count++;

            } catch (Exception ignored) {
                // 忽略极少数并发异常，保护主线程
            }
        }

        if (count == 0) return null;

        // 4. 获取全局经济数据 (如果需要，例如 Vault 总余额，可以在此 hook)
        // 这里仅示例
        double totalBalance = 0.0; 
        int onlinePlayers = Bukkit.getOnlinePlayers().size();

        // 5. 返回快照 (Record 是不可变的，且传递给 Virtual Thread 安全)
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
     */
    public synchronized void syncShops() {
        if (!Bukkit.getPluginManager().isPluginEnabled("UltimateShop")) return;
        if (ConfigManager.configManager == null || ConfigManager.configManager.shopConfigs == null) return;

        Map<String, ObjectShop> shopMap = ConfigManager.configManager.shopConfigs;
        if (shopMap.isEmpty()) return;

        List<FastItemEntry> tempEntries = new ArrayList<>(shopMap.size() * 5);
        List<String> tempIds = new ArrayList<>();
        
        PidController pidCtrl = plugin.getPidController();
        if (pidCtrl == null) return;

        int maxHandle = 0;

        // 1. 遍历并构建索引
        for (Map.Entry<String, ObjectShop> entry : shopMap.entrySet()) {
            String shopId = entry.getKey();
            ObjectShop shop = entry.getValue();
            if (shop == null || shop.getProductList() == null) continue;

            for (ObjectItem item : shop.getProductList()) {
                String productId = item.getProduct();
                if (productId == null || productId.isEmpty()) continue;

                // 构造 ID 并获取 Handle (注册 PID)
                String rawId = shopId + "_" + productId;
                int handle = pidCtrl.getHandle(rawId);
                
                // 记录最大 Handle 以便分配数组
                if (handle > maxHandle) maxHandle = handle;

                // [核心优化] 预先获取 UltimateShop 的缓存对象引用 (Direct Pointer)
                ObjectUseTimesCache cache = ShopHelper.getServerUseTimesCache(item);
                
                if (cache != null) {
                    tempEntries.add(new FastItemEntry(handle, cache));
                    tempIds.add(rawId);
                }
            }
        }

        // 2. 调整原生数组大小 (LastTotalVolumes)
        if (lastTotalVolumes.length <= maxHandle) {
            // 扩容策略：当前需求 + 256 缓冲
            double[] newVolumes = new double[maxHandle + 256];
            // 复制旧数据，防止状态丢失导致的瞬间 Delta 脉冲
            System.arraycopy(lastTotalVolumes, 0, newVolumes, 0, Math.min(lastTotalVolumes.length, newVolumes.length));
            this.lastTotalVolumes = newVolumes;
        }

        // 3. 初始化新加入物品的 LastTotal
        // 防止：新物品加入时，Total 是 1000，Last 是 0 -> Delta 瞬间变 1000 -> 经济崩溃
        for (FastItemEntry entry : tempEntries) {
             double current = entry.dataCache.getBuyUseTimes() + entry.dataCache.getSellUseTimes();
             // 只有当该 Handle 的历史数据为 0 (说明是新的或者之前没数据) 时才初始化
             if (lastTotalVolumes[entry.pidHandle] == 0) {
                 lastTotalVolumes[entry.pidHandle] = current;
             }
        }

        // 4. 原子性替换引用 (Copy-On-Write 发布)
        this.fastEntries = tempEntries.toArray(new FastItemEntry[0]);
        this.monitoredIdCache = Collections.unmodifiableList(tempIds);

        plugin.getLogger().info("[Integration] Optimized Sync: " + fastEntries.length + " items linked (Max Handle: " + maxHandle + ")");
    }

    public List<String> getMonitoredItems() {
        return monitoredIdCache;
    }
}