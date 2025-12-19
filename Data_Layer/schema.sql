-- Database schema for Apple Cloud Cost Analytics Pipeline
-- Run this script to set up the raw and gold tables

-- ============================================================
-- RAW TABLE: Accept the world as-is
-- ============================================================
-- Purpose: Store API data without rejecting any records
-- All columns are TEXT to handle inconsistent types from API
-- ingested_at tracks when each record arrived for debugging

CREATE TABLE IF NOT EXISTS raw_apple_cloud_costs (
    id              BIGSERIAL PRIMARY KEY,
    usage_date      TEXT,
    subscription_id TEXT,
    service_name    TEXT,
    cost            TEXT,  -- String because API sends "N/A", "pending", etc.
    currency        TEXT,
    team            TEXT,
    cost_center     TEXT,
    ingested_at     TIMESTAMPTZ DEFAULT now()
);

-- Index for querying recent ingestions
CREATE INDEX IF NOT EXISTS idx_raw_apple_costs_ingested
    ON raw_apple_cloud_costs (ingested_at);

COMMENT ON TABLE raw_apple_cloud_costs IS
    'Raw landing zone for Apple Cloud Cost API data. Accepts all data without validation.';

-- ============================================================
-- GOLD TABLE: Publish CFO-grade numbers
-- ============================================================
-- Purpose: Enforce business rules through database constraints
-- Typed columns, NOT NULL constraints, CHECK constraints
-- Primary key prevents duplicates at the correct grain

CREATE TABLE IF NOT EXISTS gold_apple_daily_costs (
    cost_date         DATE NOT NULL,
    subscription_id   TEXT NOT NULL,
    service_name      TEXT NOT NULL,
    team              TEXT NOT NULL,
    cost_center       TEXT NOT NULL,
    cost_usd          NUMERIC(18, 6) NOT NULL,

    -- Composite primary key: one row per date/subscription/service
    PRIMARY KEY (cost_date, subscription_id, service_name),

    -- Business rule: costs cannot be negative
    CONSTRAINT chk_cost_non_negative CHECK (cost_usd >= 0)
);

-- Index for common dashboard filters
CREATE INDEX IF NOT EXISTS idx_gold_apple_costs_date
    ON gold_apple_daily_costs (cost_date);

CREATE INDEX IF NOT EXISTS idx_gold_apple_costs_team
    ON gold_apple_daily_costs (team, cost_date);

CREATE INDEX IF NOT EXISTS idx_gold_apple_costs_subscription
    ON gold_apple_daily_costs (subscription_id, cost_date);

COMMENT ON TABLE gold_apple_daily_costs IS
    'Gold table with validated daily cloud costs. This is the internal API for the business.';

COMMENT ON COLUMN gold_apple_daily_costs.cost_usd IS
    'All costs converted to USD for consistency. Exchange rates applied during transformation.';

COMMENT ON COLUMN gold_apple_daily_costs.team IS
    'Apple team responsible for the cost. Required for allocation.';

COMMENT ON COLUMN gold_apple_daily_costs.cost_center IS
    'Finance cost center for budget tracking. Required for allocation.';

-- ============================================================
-- Verification queries
-- ============================================================
-- Run these after loading data to verify pipeline worked

-- Check raw table
-- SELECT COUNT(*) as raw_record_count FROM raw_apple_cloud_costs;
-- SELECT * FROM raw_apple_cloud_costs LIMIT 5;

-- Check gold table
-- SELECT COUNT(*) as gold_record_count FROM gold_apple_daily_costs;
-- SELECT * FROM gold_apple_daily_costs ORDER BY cost_date DESC LIMIT 10;

-- Check for data quality issues in raw
-- SELECT cost, COUNT(*)
-- FROM raw_apple_cloud_costs
-- WHERE cost IN ('N/A', 'pending', '') OR cost IS NULL
-- GROUP BY cost;

-- Business query: Total cost by team
-- SELECT team, SUM(cost_usd) as total_cost
-- FROM gold_apple_daily_costs
-- GROUP BY team
-- ORDER BY total_cost DESC;

-- Business query: Daily trend for specific team
-- SELECT cost_date, SUM(cost_usd) as daily_cost
-- FROM gold_apple_daily_costs
-- WHERE team = 'Siri Infrastructure'
-- GROUP BY cost_date
-- ORDER BY cost_date DESC
-- LIMIT 7;
