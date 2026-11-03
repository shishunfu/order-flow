CREATE TABLE inventory (
    product_id BIGINT NOT NULL,
    available_quantity INT NOT NULL,
    version BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (product_id),
    CONSTRAINT chk_inventory_quantity CHECK (available_quantity >= 0)
) ENGINE=InnoDB;

CREATE TABLE customer_order (
    id BIGINT NOT NULL AUTO_INCREMENT,
    idempotency_key VARCHAR(100) NOT NULL,
    product_id BIGINT NOT NULL,
    quantity INT NOT NULL,
    status VARCHAR(30) NOT NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (id),
    UNIQUE KEY uk_order_idempotency_key (idempotency_key),
    KEY idx_order_created_at (created_at),
    CONSTRAINT chk_order_quantity CHECK (quantity > 0)
) ENGINE=InnoDB;

INSERT INTO inventory(product_id, available_quantity)
VALUES
(1001, 100),
(1002, 50);
