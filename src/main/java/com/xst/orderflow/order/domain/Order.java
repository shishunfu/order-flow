package com.xst.orderflow.order.domain;

public record Order(Long id, String idempotencyKey, Long productId, int quantity, OrderStatus status) {
    public static Order create(String key, Long productId, int quantity) {
        return new Order(null, key, productId, quantity, OrderStatus.CREATED);
    }
}
