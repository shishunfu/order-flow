package com.xst.orderflow.inventory.application;

import com.xst.orderflow.inventory.domain.InventoryRepository;
import com.xst.orderflow.shared.exception.BusinessException;
import org.springframework.stereotype.Service;

@Service
public class InventoryApplicationService {
    private final InventoryRepository repository;

    public InventoryApplicationService(InventoryRepository repository) {
        this.repository = repository;
    }

    public void deduct(Long productId, int quantity) {
        if (!repository.deduct(productId, quantity)) {
            throw new BusinessException("INVENTORY_NOT_ENOUGH", "商品不存在或库存不足");
        }
    }
}
