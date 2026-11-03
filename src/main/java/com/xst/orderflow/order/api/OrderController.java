package com.xst.orderflow.order.api;

import com.xst.orderflow.order.application.*;
import com.xst.orderflow.order.domain.Order;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/orders")
public class OrderController {
    private final OrderApplicationService service;

    public OrderController(OrderApplicationService service) {
        this.service = service;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Order create(@RequestHeader("Idempotency-Key") String key,
                        @Valid @RequestBody CreateOrderRequest request) {
        return service.create(key, request);
    }

    @GetMapping("/{id}")
    public Order get(@PathVariable Long id) {
        return service.get(id);
    }
}
