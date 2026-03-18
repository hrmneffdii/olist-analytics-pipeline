-- DIM SELLERS
-- 1. CREATE STAGING TABLE


CREATE TABLE IF NOT EXISTS silver.stg_sellers (
    seller_id TEXT,	
    seller_zip_code_prefix TEXT,
    seller_city	TEXT,
    seller_state TEXT,	
    ingested_at TIMESTAMP
);

-- RESET STAGING

TRUNCATE TABLE silver.stg_sellers;


-- 2. INCREMENTAL LOAD FROM BRONZE → STAGING

INSERT INTO silver.stg_sellers (
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    ingested_at
)

WITH bronze_incremental AS (
    SELECT
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state,
        ingested_at
    FROM bronze.sellers 
    WHERE ingested_at >= (
        SELECT COALESCE(MAX(ingested_at), TIMESTAMP '1900-01-01')
        FROM silver.stg_sellers
    )
),

ranked_sellers AS (
    SELECT
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state,
        ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY seller_id
            ORDER BY ingested_at DESC
        ) AS rn
    FROM bronze_incremental
),

deduplicated AS (
    SELECT
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state,
        ingested_at
    FROM ranked_sellers
    WHERE rn = 1
)


SELECT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    ingested_at
FROM deduplicated;


-- 3. CREATE DIMENSION TABLE

CREATE TABLE IF NOT EXISTS silver.dim_sellers (
    seller_key BIGINT PRIMARY KEY,
    seller_id TEXT,	
    seller_zip_code_prefix TEXT,
    seller_city	TEXT,
    seller_state TEXT,	
    ingested_at TIMESTAMP,
    CONSTRAINT uq_seller UNIQUE(seller_id)
);


-- 4. CREATE SEQUENCE FOR SURROGATE KEY

CREATE SEQUENCE IF NOT EXISTS silver.seller_key_seq;


-- 5. INCREMENTAL MERGE INTO DIMENSION

MERGE INTO silver.dim_sellers AS target
USING silver.stg_sellers AS source

ON target.seller_id = source.seller_id

WHEN NOT MATCHED THEN
INSERT (
    seller_key,
    seller_id,	
    seller_zip_code_prefix,
    seller_city,
    seller_state,	
    ingested_at 
)
VALUES (
    nextval('silver.seller_key_seq'),
    source.seller_id,
    source.seller_zip_code_prefix,
    source.seller_city,
    source.seller_state,
    source.ingested_at
);


-- 6. INDEXES (IDEMPOTENT)

CREATE INDEX IF NOT EXISTS idx_stg_sellers_ingested
ON silver.stg_sellers(ingested_at);

CREATE INDEX IF NOT EXISTS idx_dim_sellers
ON silver.dim_sellers(seller_id);