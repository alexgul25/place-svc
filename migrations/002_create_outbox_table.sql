-- +goose Up
CREATE TABLE IF NOT EXISTS outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    msg_topic VARCHAR(50) NOT NULL,
    msg_key TEXT,
    msg_payload JSONB NOT NULL,
    send_status VARCHAR(50) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);

-- +goose Down
DROP TABLE IF EXISTS outbox;