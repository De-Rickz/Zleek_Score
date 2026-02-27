-- 1) Features Table (5-minute rolling calculations)
CREATE TABLE IF NOT EXISTS features_5m (
    market_id TEXT REFERENCES markets(id),
    bucket TIMESTAMPTZ NOT NULL,
    price_z_score NUMERIC,
    ob_imbalance NUMERIC,
    PRIMARY KEY (market_id, bucket)
);

-- Turn the features table into a Hypertable too!
SELECT create_hypertable('features_5m', 'bucket', if_not_exists => TRUE);

-- 2) Tremor Signals Table (The final 0-100 score)
CREATE TABLE IF NOT EXISTS signals_history (
    market_id TEXT REFERENCES markets(id),
    ts TIMESTAMPTZ NOT NULL,
    tremor_score NUMERIC CHECK (tremor_score >= 0 AND tremor_score <= 100),
    PRIMARY KEY (market_id, ts)
);

-- 3) Alert Rules (What the user wants to be notified about)
CREATE TABLE IF NOT EXISTS alert_rules (
    id SERIAL PRIMARY KEY,
    market_id TEXT REFERENCES markets(id),
    metric TEXT NOT NULL,
    operator TEXT NOT NULL,
    threshold NUMERIC NOT NULL,
    is_active BOOLEAN DEFAULT TRUE
);