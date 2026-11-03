package com.xst.orderflow.inventory.infrastructure;

import com.xst.orderflow.inventory.domain.InventoryRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class JdbcInventoryRepository implements InventoryRepository {
    private final JdbcTemplate jdbc;
    public JdbcInventoryRepository(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    @Override
    public boolean deduct(Long productId, int quantity) {
        int rows = jdbc.update(
                "UPDATE inventory SET available_quantity = available_quantity - ?, version = version + 1 " +
                "WHERE product_id = ? AND available_quantity >= ?",
                quantity, productId, quantity);
        return rows == 1;
    }
}
