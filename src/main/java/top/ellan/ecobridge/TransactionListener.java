package top.ellan.ecobridge;

import cn.superiormc.ultimateshop.api.ItemFinishTransactionEvent;
import cn.superiormc.ultimateshop.objects.buttons.ObjectItem;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;

/**
 * UltimateShop 交易事件监听器
 * 
 * 功能：监听交易完成事件，获取实际交易数量并存入缓存
 * 
 * API 说明（基于 ItemFinishTransactionEvent 反编译源码）：
 *   - getAmount(): 返回交易次数 (multi)
 *   - isBuyOrSell(): true = 买入, false = 卖出
 *   - getItem(): 返回 ObjectItem 对象
 *   - getPlayer(): 返回 Player 对象
 * 
 * 实际数量计算：
 *   actualAmount = multi × item.getDisplayItemObject().getAmountPlaceholder(player)
 * 
 * 来源参考：
 *   - BuyProductMethod.java:166-170
 *   - SellProductMethod.java:206-210
 */
public class TransactionListener implements Listener {

    private final EcoBridge plugin;

    public TransactionListener(EcoBridge plugin) {
        this.plugin = plugin;
    }

    /**
     * 监听交易完成事件
     * 
     * 优先级：MONITOR（在交易完全完成后执行，不影响交易流程）
     * 忽略已取消的事件（如果交易被取消，不记录）
     */
    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onTransactionFinish(ItemFinishTransactionEvent event) {
        // 获取交易信息
        Player player = event.getPlayer();
        ObjectItem item = event.getItem();
        int multi = event.getAmount();           // 交易次数
        boolean isBuy = event.isBuyOrSell();     // true = 买入, false = 卖出

        // 安全检查
        if (player == null || item == null) return;

        // 获取物品ID (格式: shopId_productId)
        String itemId = generateItemId(item);
        if (itemId == null || itemId.isEmpty()) return;

        // 计算实际交易数量
        // 公式来源: BuyProductMethod.java:166-170, SellProductMethod.java:206-210
        // calculateAmount = multi * item.getDisplayItemObject().getAmountPlaceholder(player)
        int amountPlaceholder = item.getDisplayItemObject().getAmountPlaceholder(player);
        long actualAmount = (long) multi * amountPlaceholder;

        // 获取缓存实例
        TransactionAmountCache cache = TransactionAmountCache.getInstance();

        // 根据交易类型记录数量
        if (isBuy) {
            // isBuyOrSell() = true → 玩家从商店购买物品
            // 商店库存减少 → 需求增加 → 价格应上涨
            cache.recordBuy(itemId, actualAmount);
            
            if (plugin.getConfig().getBoolean("debug", false)) {
                plugin.getLogger().info(String.format(
                    "[Transaction] BUY: %s bought %d x %s from shop (multi=%d, placeholder=%d)",
                    player.getName(), actualAmount, itemId, multi, amountPlaceholder
                ));
            }
        } else {
            // isBuyOrSell() = false → 玩家向商店出售物品
            // 商店库存增加 → 供应增加 → 价格应下降
            cache.recordSell(itemId, actualAmount);
            
            if (plugin.getConfig().getBoolean("debug", false)) {
                plugin.getLogger().info(String.format(
                    "[Transaction] SELL: %s sold %d x %s to shop (multi=%d, placeholder=%d)",
                    player.getName(), actualAmount, itemId, multi, amountPlaceholder
                ));
            }
        }
    }

    /**
     * 生成物品唯一标识符
     * 
     * 格式: shopId_productId
     * 示例: main_diamond, minerals_iron_ingot
     * 
     * @param item 商品对象
     * @return 物品ID字符串
     */
    private String generateItemId(ObjectItem item) {
        try {
            // getShop() 直接返回 String 类型
            String shopId = item.getShop();
            if (shopId == null || shopId.isEmpty()) {
                shopId = "unknown";
            }
            
            // 获取产品ID
            String productId = item.getProduct();
            if (productId == null || productId.isEmpty()) {
                productId = "unknown";
            }
            
            return shopId + "_" + productId;
        } catch (Exception e) {
            plugin.getLogger().warning("[Transaction] Failed to generate item ID: " + e.getMessage());
            return null;
        }
    }
}
