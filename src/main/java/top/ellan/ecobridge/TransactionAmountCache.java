package top.ellan.ecobridge;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * 交易数量缓存管理器
 * 
 * 功能：存储每个物品的实际交易数量（而非交易次数）
 * 
 * 数据结构：
 *   - Key: shopId_productId (物品唯一标识)
 *   - Value: AmountCounter (包含买入和卖出数量的累加器)
 * 
 * 工作原理：
 *   1. TransactionListener 监听 UltimateShop 的交易完成事件
 *   2. 从事件中获取实际交易数量 (multi * amountPlaceholder)
 *   3. 存入此缓存，按 shopId_productId 分类
 *   4. IntegrationManager 在 captureDataSnapshot 时读取并计算增量
 */
public class TransactionAmountCache {

    /**
     * 数量计数器 - 使用 LongAdder 实现高并发写入
     */
    public static class AmountCounter {
        private final LongAdder sellAmount = new LongAdder();  // 累计卖出数量
        private final LongAdder buyAmount = new LongAdder();   // 累计买入数量

        public void addSell(long amount) {
            sellAmount.add(amount);
        }

        public void addBuy(long amount) {
            buyAmount.add(amount);
        }

        public long getSellAmount() {
            return sellAmount.sum();
        }

        public long getBuyAmount() {
            return buyAmount.sum();
        }

        /**
         * 获取净流量 = 卖出 - 买入
         * 正值：玩家向商店卖出了更多物品（供应增加，价格应下降）
         * 负值：玩家从商店买入了更多物品（需求增加，价格应上涨）
         */
        public long getNetFlow() {
            return sellAmount.sum() - buyAmount.sum();
        }

        public void reset() {
            sellAmount.reset();
            buyAmount.reset();
        }
    }

    // ==================== 实例字段 ====================
    
    private final ConcurrentHashMap<String, AmountCounter> cache = new ConcurrentHashMap<>(4096);
    
    // 单例模式
    private static volatile TransactionAmountCache instance;
    
    public static TransactionAmountCache getInstance() {
        if (instance == null) {
            synchronized (TransactionAmountCache.class) {
                if (instance == null) {
                    instance = new TransactionAmountCache();
                }
            }
        }
        return instance;
    }

    private TransactionAmountCache() {
        // 私有构造函数
    }

    /**
     * 获取或创建指定物品的计数器
     * 
     * @param itemId 物品ID (格式: shopId_productId)
     * @return 该物品的数量计数器
     */
    public AmountCounter getOrCreate(String itemId) {
        return cache.computeIfAbsent(itemId, k -> new AmountCounter());
    }

    /**
     * 获取指定物品的计数器（不创建）
     * 
     * @param itemId 物品ID
     * @return 计数器，如果不存在则返回 null
     */
    public AmountCounter get(String itemId) {
        return cache.get(itemId);
    }

    /**
     * 记录一次卖出交易
     * 
     * @param itemId 物品ID (格式: shopId_productId)
     * @param amount 实际卖出的物品数量
     */
    public void recordSell(String itemId, long amount) {
        getOrCreate(itemId).addSell(amount);
    }

    /**
     * 记录一次买入交易
     * 
     * @param itemId 物品ID (格式: shopId_productId)
     * @param amount 实际买入的物品数量
     */
    public void recordBuy(String itemId, long amount) {
        getOrCreate(itemId).addBuy(amount);
    }

    /**
     * 获取所有缓存的物品ID
     * 
     * @return 物品ID集合
     */
    public java.util.Set<String> getCachedItemIds() {
        return cache.keySet();
    }

    /**
     * 清空所有缓存
     * 通常在服务器关闭或重载时调用
     */
    public void clear() {
        cache.clear();
    }

    /**
     * 获取缓存大小
     * 
     * @return 缓存中的物品数量
     */
    public int size() {
        return cache.size();
    }
}
