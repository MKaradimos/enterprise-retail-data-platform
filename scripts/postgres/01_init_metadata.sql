-- ============================================================
-- RetailNova Metadata Database (PostgreSQL)
-- Stores: watermarks, data quality log, execution log,
--         data quality rules, pipeline metrics
-- ============================================================

-- ─── DATA QUALITY RULES TABLE ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS data_quality_rules (
    rule_id         SERIAL          PRIMARY KEY,
    rule_name       VARCHAR(200)    NOT NULL,
    target_table    VARCHAR(200)    NOT NULL,
    target_column   VARCHAR(200),
    rule_type       VARCHAR(50)     NOT NULL,  -- not_null, unique, range, regex, freshness, row_count
    rule_params     JSONB,
    threshold_pct   DECIMAL(5,2)    DEFAULT 100.0,
    severity        VARCHAR(20)     DEFAULT 'WARNING',  -- INFO, WARNING, CRITICAL
    is_active       BOOLEAN         DEFAULT TRUE,
    created_at      TIMESTAMP       DEFAULT NOW(),
    updated_at      TIMESTAMP       DEFAULT NOW()
);

-- ─── DATA QUALITY LOG ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS data_quality_log (
    log_id              SERIAL          PRIMARY KEY,
    rule_id             INT             REFERENCES data_quality_rules(rule_id),
    run_id              VARCHAR(100)    NOT NULL,
    pipeline_name       VARCHAR(200),
    layer               VARCHAR(20),    -- bronze, silver, gold
    target_table        VARCHAR(200)    NOT NULL,
    target_column       VARCHAR(200),
    rule_type           VARCHAR(50),
    total_records       BIGINT,
    passed_records      BIGINT,
    failed_records      BIGINT,
    pass_rate_pct       DECIMAL(7,4),
    threshold_pct       DECIMAL(5,2),
    status              VARCHAR(20),    -- PASS, FAIL, WARNING
    severity            VARCHAR(20),
    details             JSONB,
    run_at              TIMESTAMP       DEFAULT NOW()
);

-- ─── EXECUTION LOG ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS execution_log (
    log_id              SERIAL          PRIMARY KEY,
    run_id              VARCHAR(100)    NOT NULL,
    pipeline_name       VARCHAR(200)    NOT NULL,
    pipeline_type       VARCHAR(50),    -- master, ingestion, transformation, quality
    layer               VARCHAR(20),
    source_table        VARCHAR(200),
    target_table        VARCHAR(200),
    status              VARCHAR(30)     NOT NULL,  -- RUNNING, SUCCESS, FAILED, SKIPPED
    rows_read           BIGINT          DEFAULT 0,
    rows_written        BIGINT          DEFAULT 0,
    rows_skipped        BIGINT          DEFAULT 0,
    rows_failed         BIGINT          DEFAULT 0,
    started_at          TIMESTAMP       NOT NULL DEFAULT NOW(),
    completed_at        TIMESTAMP,
    duration_seconds    DECIMAL(10,3),
    error_message       TEXT,
    metadata            JSONB
);

-- ─── ERROR LOG ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS error_log (
    error_id            SERIAL          PRIMARY KEY,
    run_id              VARCHAR(100),
    pipeline_name       VARCHAR(200),
    error_type          VARCHAR(100),   -- DataQuality, SchemaValidation, Connection, Transform
    severity            VARCHAR(20),    -- INFO, WARNING, ERROR, CRITICAL
    error_message       TEXT,
    stack_trace         TEXT,
    source_table        VARCHAR(200),
    target_table        VARCHAR(200),
    record_count        BIGINT,
    is_resolved         BOOLEAN         DEFAULT FALSE,
    resolution_notes    TEXT,
    occurred_at         TIMESTAMP       DEFAULT NOW(),
    resolved_at         TIMESTAMP
);

-- ─── PIPELINE METRICS ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    metric_id           SERIAL          PRIMARY KEY,
    run_id              VARCHAR(100)    NOT NULL,
    pipeline_name       VARCHAR(200)    NOT NULL,
    metric_date         DATE            NOT NULL DEFAULT CURRENT_DATE,
    metric_name         VARCHAR(200)    NOT NULL,
    metric_value        DECIMAL(20,4),
    metric_unit         VARCHAR(50),    -- seconds, rows, bytes, percent
    layer               VARCHAR(20),
    recorded_at         TIMESTAMP       DEFAULT NOW()
);

-- ─── WATERMARK TABLE ──────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline_watermarks (
    table_name          VARCHAR(200)    NOT NULL PRIMARY KEY,
    last_watermark      TIMESTAMP       NOT NULL DEFAULT '1900-01-01 00:00:00',
    last_run_at         TIMESTAMP,
    rows_extracted      BIGINT          DEFAULT 0,
    status              VARCHAR(20)     DEFAULT 'Never Run',
    updated_at          TIMESTAMP       DEFAULT NOW()
);

-- ─── SCD2 CHANGE TRACKING ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS scd2_change_log (
    change_id           SERIAL          PRIMARY KEY,
    source_table        VARCHAR(200)    NOT NULL,
    natural_key         VARCHAR(500),
    changed_columns     JSONB,
    old_values          JSONB,
    new_values          JSONB,
    change_type         VARCHAR(20),    -- INSERT, UPDATE, DELETE
    detected_at         TIMESTAMP       DEFAULT NOW(),
    processed           BOOLEAN         DEFAULT FALSE,
    processed_at        TIMESTAMP
);

-- ─── SCHEMA VALIDATION LOG ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS schema_validation_log (
    validation_id       SERIAL          PRIMARY KEY,
    run_id              VARCHAR(100),
    table_name          VARCHAR(200)    NOT NULL,
    layer               VARCHAR(20),
    expected_columns    JSONB,
    actual_columns      JSONB,
    missing_columns     JSONB,
    extra_columns       JSONB,
    status              VARCHAR(20),
    validated_at        TIMESTAMP       DEFAULT NOW()
);

-- ─── SEED DATA QUALITY RULES ──────────────────────────────────────────────
INSERT INTO data_quality_rules (rule_name, target_table, target_column, rule_type, rule_params, threshold_pct, severity) VALUES
-- Not Null rules
('customers_email_not_null',        'silver_customers',    'email',         'not_null',  NULL,                               100.0, 'CRITICAL'),
('customers_country_not_null',      'silver_customers',    'country',       'not_null',  NULL,                               100.0, 'CRITICAL'),
('products_price_not_null',         'silver_products',     'unit_price',    'not_null',  NULL,                               100.0, 'CRITICAL'),
('orders_customer_id_not_null',     'silver_sales_orders', 'customer_id',   'not_null',  NULL,                               100.0, 'CRITICAL'),
('orders_order_date_not_null',      'silver_sales_orders', 'order_date',    'not_null',  NULL,                               100.0, 'CRITICAL'),

-- Uniqueness rules
('customers_email_unique',          'silver_customers',    'email',         'unique',    NULL,                               100.0, 'CRITICAL'),
('products_code_unique',            'silver_products',     'product_code',  'unique',    NULL,                               100.0, 'CRITICAL'),
('orders_number_unique',            'silver_sales_orders', 'order_number',  'unique',    NULL,                               100.0, 'CRITICAL'),

-- Range rules
('products_price_positive',         'silver_products',     'unit_price',    'range',     '{"min": 0.01, "max": 99999.99}', 100.0, 'WARNING'),
('orders_discount_range',           'silver_sales_orders', 'discount_pct',  'range',     '{"min": 0, "max": 100}',         100.0, 'WARNING'),
('orderlines_qty_positive',         'silver_order_lines',  'quantity',      'range',     '{"min": 1, "max": 9999}',        100.0, 'WARNING'),

-- Regex rules
('customers_email_format',          'silver_customers',    'email',         'regex',     '{"pattern": "^[^@]+@[^@]+\\.[^@]+$"}', 99.0, 'WARNING'),
('customers_phone_format',          'silver_customers',    'phone',         'regex',     '{"pattern": "^\\+?[0-9\\-\\s]{7,20}$"}', 90.0, 'INFO'),

-- Row count rules
('customers_min_rows',              'silver_customers',    NULL,            'row_count', '{"min_rows": 1}',                100.0, 'CRITICAL'),
('orders_min_rows',                 'silver_sales_orders', NULL,            'row_count', '{"min_rows": 1}',                100.0, 'CRITICAL'),

-- Freshness rules
('orders_freshness',                'silver_sales_orders', 'order_date',    'freshness', '{"max_age_days": 2}',            100.0, 'WARNING'),

-- Completeness rules
('customers_completeness',          'silver_customers',    'phone',         'completeness', '{"threshold_pct": 80}',        80.0, 'INFO'),
('orders_completeness',             'silver_sales_orders', 'payment_method','completeness', '{"threshold_pct": 95}',        95.0, 'WARNING');

-- ─── SEED WATERMARKS ──────────────────────────────────────────────────────
INSERT INTO pipeline_watermarks (table_name, last_watermark) VALUES
('src_customers',           '1900-01-01 00:00:00'),
('src_products',            '1900-01-01 00:00:00'),
('src_stores',              '1900-01-01 00:00:00'),
('src_sales_orders',        '1900-01-01 00:00:00'),
('src_sales_order_lines',   '1900-01-01 00:00:00')
ON CONFLICT (table_name) DO NOTHING;

-- ─── INDEXES ──────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_quality_log_run    ON data_quality_log(run_id);
CREATE INDEX IF NOT EXISTS idx_quality_log_table  ON data_quality_log(target_table);
CREATE INDEX IF NOT EXISTS idx_quality_log_status ON data_quality_log(status);
CREATE INDEX IF NOT EXISTS idx_exec_log_run       ON execution_log(run_id);
CREATE INDEX IF NOT EXISTS idx_exec_log_pipeline  ON execution_log(pipeline_name);
CREATE INDEX IF NOT EXISTS idx_exec_log_status    ON execution_log(status);
CREATE INDEX IF NOT EXISTS idx_error_log_run      ON error_log(run_id);
CREATE INDEX IF NOT EXISTS idx_metrics_date       ON pipeline_metrics(metric_date);

SELECT 'Metadata DB initialized successfully' AS status;
