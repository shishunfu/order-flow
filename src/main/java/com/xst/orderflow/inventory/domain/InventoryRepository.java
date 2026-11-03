package com.xst.orderflow.inventory.domain;

public interface InventoryRepository {
    boolean deduct(Long productId, int quantity);
}
