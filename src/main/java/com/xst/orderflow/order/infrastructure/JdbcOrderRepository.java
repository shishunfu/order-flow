package com.xst.orderflow.order.infrastructure;

import com.xst.orderflow.order.domain.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Repository;
import java.sql.Statement;
import java.util.Optional;

@Repository
public class JdbcOrderRepository implements OrderRepository {
    private static final RowMapper<Order> MAPPER = (rs, n) -> new Order(
            rs.getLong("id"), rs.getString("idempotency_key"), rs.getLong("product_id"),
            rs.getInt("quantity"), OrderStatus.valueOf(rs.getString("status")));
    private final JdbcTemplate jdbc;
    public JdbcOrderRepository(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    @Override
    public Order save(Order order) {
        var holder = new GeneratedKeyHolder();
        jdbc.update(connection -> {
            var ps = connection.prepareStatement(
                    "INSERT INTO customer_order(idempotency_key, product_id, quantity, status) VALUES(?,?,?,?)",
                    Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, order.idempotencyKey());
            ps.setLong(2, order.productId());
            ps.setInt(3, order.quantity());
            ps.setString(4, order.status().name());
            return ps;
        }, holder);
        Number key = holder.getKey();
        return new Order(key == null ? null : key.longValue(), order.idempotencyKey(),
                order.productId(), order.quantity(), order.status());
    }

    @Override
    public Optional<Order> findById(Long id) {
        return jdbc.query("SELECT * FROM customer_order WHERE id=?", MAPPER, id).stream().findFirst();
    }
    @Override
    public Optional<Order> findByIdempotencyKey(String key) {
        return jdbc.query("SELECT * FROM customer_order WHERE idempotency_key=?", MAPPER, key)
                .stream().findFirst();
    }
}
