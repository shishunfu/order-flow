package com.xst.orderflow.order;


import com.xst.orderflow.inventory.application.InventoryApplicationService;
import com.xst.orderflow.inventory.domain.InventoryRepository;
import com.xst.orderflow.order.application.*;
import com.xst.orderflow.order.domain.*;
import com.xst.orderflow.shared.exception.BusinessException;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class OrderApplicationServiceTest {
    @Test
    void createsOrderWhenStockIsEnough() {
        var inventoryRepo = mock(InventoryRepository.class);
        var orderRepo = mock(OrderRepository.class);
        when(inventoryRepo.deduct(1001L, 2)).thenReturn(true);
        when(orderRepo.findByIdempotencyKey("key-1")).thenReturn(Optional.empty());
        when(orderRepo.save(any())).thenAnswer(inv -> {
            Order o = inv.getArgument(0);
            return new Order(1L, o.idempotencyKey(), o.productId(), o.quantity(), o.status());
        });
        var service = new OrderApplicationService(new InventoryApplicationService(inventoryRepo), orderRepo);
        assertEquals(1L, service.create("key-1", new CreateOrderRequest(1001L, 2)).id());
        verify(inventoryRepo).deduct(1001L, 2);
        verify(orderRepo).save(any());
    }

    @Test
    void failsWhenStockIsInsufficient() {
        var inventoryRepo = mock(InventoryRepository.class);
        var orderRepo = mock(OrderRepository.class);
        when(inventoryRepo.deduct(1001L, 2)).thenReturn(false);
        var service = new OrderApplicationService(new InventoryApplicationService(inventoryRepo), orderRepo);
        assertThrows(BusinessException.class,
            () -> service.create("key-2", new CreateOrderRequest(1001L, 2)));
        verify(orderRepo, never()).save(any());
    }
}
