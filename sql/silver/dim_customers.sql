-- DIM CUSTOMER
-- 1. CREATE STAGING TABLE

CREATE TABLE IF NOT EXISTS silver.stg_customers (
    customer_id TEXT,
    customer_unique_id TEXT,
    customer_zip_code_prefix TEXT,
    customer_city TEXT,
    customer_state TEXT,
    ingested_at TIMESTAMP
);

-- RESET STAGING

TRUNCATE TABLE silver.stg_customers;


-- 2. INCREMENTAL LOAD FROM BRONZE → STAGING

INSERT INTO silver.stg_customers (
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    ingested_at
)

WITH bronze_incremental AS (
    SELECT
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        ingested_at
    FROM bronze.customers
    WHERE ingested_at >= (
        SELECT COALESCE(MAX(ingested_at), TIMESTAMP '1900-01-01')
        FROM silver.stg_customers
    )
),

ranked_customers AS (
    SELECT
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY customer_unique_id
            ORDER BY ingested_at DESC
        ) AS rn
    FROM bronze_incremental
),

deduplicated AS (
    SELECT
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        ingested_at
    FROM ranked_customers
    WHERE rn = 1
)

SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    ingested_at
FROM deduplicated;


-- 3. CREATE DIMENSION TABLE

CREATE TABLE IF NOT EXISTS silver.dim_customers (
    customer_key BIGINT PRIMARY KEY,
    customer_id TEXT,
    customer_unique_id TEXT,
    customer_zip_code_prefix TEXT,
    customer_city TEXT,
    customer_state TEXT,
    ingested_at TIMESTAMP,
    CONSTRAINT uq_customer UNIQUE(customer_unique_id)
);


-- 4. CREATE SEQUENCE FOR SURROGATE KEY

CREATE SEQUENCE IF NOT EXISTS silver.customer_key_seq;


-- 5. INCREMENTAL MERGE INTO DIMENSION

MERGE INTO silver.dim_customers AS target
USING silver.stg_customers AS source

ON target.customer_unique_id = source.customer_unique_id

WHEN NOT MATCHED THEN
INSERT (
    customer_key,
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    ingested_at
)
VALUES (
    nextval('silver.customer_key_seq'),
    source.customer_id,
    source.customer_unique_id,
    source.customer_zip_code_prefix,
    source.customer_city,
    source.customer_state,
    source.ingested_at
);


-- 6. INDEXES (IDEMPOTENT)

CREATE INDEX IF NOT EXISTS idx_stg_customers_ingested_at
ON silver.stg_customers(ingested_at);

CREATE INDEX IF NOT EXISTS idx_dim_customers_unique
ON silver.dim_customers(customer_unique_id);