ALTER TABLE payments
    ADD CONSTRAINT uk_payments_order_id UNIQUE (order_id),
    ADD CONSTRAINT chk_payments_amount CHECK (amount > 0),
    ADD CONSTRAINT chk_payments_status CHECK (status IN ('PENDING', 'APPROVED', 'FAILED', 'REFUNDED'));

ALTER TABLE payment_attempts
    ADD CONSTRAINT fk_payment_attempts_payment_id FOREIGN KEY (payment_id) REFERENCES payments(payment_id),
    ADD CONSTRAINT uk_payment_attempts_payment_number UNIQUE (payment_id, attempt_number),
    ADD CONSTRAINT chk_payment_attempts_number CHECK (attempt_number > 0),
    ADD CONSTRAINT chk_payment_attempts_result CHECK (result IN ('PENDING', 'APPROVED', 'FAILED', 'REFUNDED'));

CREATE INDEX idx_payments_customer_id ON payments (customer_id);
CREATE INDEX idx_payments_status ON payments (status);

ALTER TABLE inbox_events
    ADD COLUMN aggregate_id VARCHAR(100),
    ADD COLUMN correlation_id VARCHAR(100),
    ADD COLUMN causation_id VARCHAR(100),
    ADD COLUMN producer VARCHAR(100),
    ADD COLUMN source_topic VARCHAR(200);

CREATE INDEX idx_inbox_events_correlation_id ON inbox_events (correlation_id);
