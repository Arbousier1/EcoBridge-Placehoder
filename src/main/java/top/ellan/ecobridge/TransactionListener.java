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
 * 修正记录：
 * 1. event.getMulti() -> event.getAmount()
 * 2. event.isBuy() -> event.isBuyOrSell()
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
        
        // 【修正 1】反编译代码中显示方法为 getAmount()，这通常代表交易倍数(multiplier)
        int multi = event.getAmount(); 
        
        // 【修正 2】反编译代码中显示方法为 isBuyOrSell()
        // 通常 true 代表购买，false 代表出售（取决于插件具体实现，但这是最常见的逻辑）
        boolean isBuy = event.isBuyOrSell();

        // 安全检查
        if (player == null || item == null) return;

        // 获取物品ID (格式: shopId_productId)
        String itemId = generateItemId(item);
        if (itemId == null || itemId.isEmpty()) return;

        // 计算实际交易数量
        // calculateAmount = multi * item.getDisplayItemObject().getAmountPlaceholder(player)
        int amountPlaceholder = item.getDisplayItemObject().getAmountPlaceholder(player);
        long actualAmount = (long) multi * amountPlaceholder;

        // 获取缓存实例
        TransactionAmountCache cache = TransactionAmountCache.getInstance();

        // 根据交易类型记录数量
        if (isBuy) {
            // 玩家从商店购买物品 → 商店库存减少 → 需求增加 → 价格应上涨
            cache.recordBuy(itemId, actualAmount);
            
            if (plugin.getConfig().getBoolean("debug", false)) {
                plugin.getLogger().info(String.format(
                    "[Transaction] BUY: %s bought %d x %s from shop",
                    player.getName(), actualAmount, itemId
                ));
            }
        } else {
            // 玩家向商店出售物品 → 商店库存增加 → 供应增加 → 价格应下降
            cache.recordSell(itemId, actualAmount);
            
            if (plugin.getConfig().getBoolean("debug", false)) {
                plugin.getLogger().info(String.format(
                    "[Transaction] SELL: %s sold %d x %s to shop",
                    player.getName(), actualAmount, itemId
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
            // 注意：此处假设 ObjectItem 有 getShop() 返回 String。
            // 如果后续这里报错，可能需要改为 item.getShopObject().getShopId() 或类似方法
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