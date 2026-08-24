CREATE TABLE payments (
    id BIGSERIAL PRIMARY KEY,
    payment_id VARCHAR(100) NOT NULL UNIQUE,
    order_id BIGINT NOT NULL,
    customer_id BIGINT NOT NULL,
    amount DECIMAL(12, 2) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    status VARCHAR(30) NOT NULL,
    failure_reason VARCHAR(500),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ
);

CREATE TABLE payment_attempts (
    id BIGSERIAL PRIMARY KEY,
    payment_id VARCHAR(100) NOT NULL,
    attempt_number INT NOT NULL,
    result VARCHAR(30) NOT NULL,
    error_message VARCHAR(500),
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE processed_events (
    event_id VARCHAR(100) PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL
);
