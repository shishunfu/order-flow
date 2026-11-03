package com.xst.orderflow.order.application;

import com.xst.orderflow.inventory.application.InventoryApplicationService;
import com.xst.orderflow.order.domain.*;
import com.xst.orderflow.shared.exception.BusinessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderApplicationService {
    private final InventoryApplicationService inventory;
    private final OrderRepository orders;

    public OrderApplicationService(InventoryApplicationService inventory, OrderRepository orders) {
        this.inventory = inventory;
        this.orders = orders;
    }

    @Transactional
    public Order create(String key, CreateOrderRequest request) {
        var existing = orders.findByIdempotencyKey(key);
        if (existing.isPresent()) return existing.get();

        inventory.deduct(request.productId(), request.quantity());
        try {
            return orders.save(Order.create(key, request.productId(), request.quantity()));
        } catch (DuplicateKeyException ex) {
            // 并发相同幂等键的最终保护由唯一索引完成；抛出运行时异常使本事务回滚。
            throw new BusinessException("DUPLICATE_REQUEST", "请求发生并发冲突，请查询订单确认结果");
        }
    }

    public Order get(Long id) {
        return orders.findById(id)
            .orElseThrow(() -> new BusinessException("ORDER_NOT_FOUND", "订单不存在"));
    }
}
