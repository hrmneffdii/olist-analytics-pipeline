-- DIM PRODUCTS
-- 1. CREATE STAGING TABLE


CREATE TABLE IF NOT EXISTS silver.stg_products (
    product_id TEXT,
    product_category_name TEXT,
    product_name_lenght INTEGER,
    product_description_lenght INTEGER,
    product_photos_qty INTEGER,
    product_weight_g NUMERIC(10,2),
    product_length_cm NUMERIC(10,2),
    product_height_cm NUMERIC(10,2),
    product_width_cm NUMERIC(10,2),
    ingested_at TIMESTAMP
);


-- RESET STAGING

TRUNCATE TABLE silver.stg_products;


-- 2. INCREMENTAL LOAD FROM BRONZE → STAGING

INSERT INTO silver.stg_products (
    product_id,
    product_category_name,
    product_name_lenght,
    product_description_lenght,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    ingested_at
)

WITH bronze_incremental AS (
    SELECT
        product_id,
        product_category_name,
        product_name_lenght,
        product_description_lenght,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,
        ingested_at
    FROM bronze.products
    WHERE ingested_at >= (
        SELECT COALESCE(MAX(ingested_at), TIMESTAMP '1900-01-01')
        FROM silver.stg_products
    )    
),

ranked_products AS (
    SELECT
        product_id,
        product_category_name,
        product_name_lenght,
        product_description_lenght,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,
        ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY ingested_at DESC
        ) AS rn
    FROM bronze_incremental
),

deduplicated AS (
    SELECT
        product_id,
        product_category_name,
        product_name_lenght,
        product_description_lenght,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,
        ingested_at
    FROM ranked_products
    WHERE rn = 1
)

SELECT
    product_id,
    product_category_name,
    product_name_lenght,
    product_description_lenght,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    ingested_at
FROM deduplicated;


-- 3. CREATE DIMENSION TABLE

CREATE TABLE IF NOT EXISTS silver.dim_products (
    product_key BIGINT PRIMARY KEY,
    product_id TEXT,
    product_category_name TEXT,
    product_name_lenght INTEGER,
    product_description_lenght INTEGER,
    product_photos_qty INTEGER,
    product_weight_g NUMERIC(10,2),
    product_length_cm NUMERIC(10,2),
    product_height_cm NUMERIC(10,2),
    product_width_cm NUMERIC(10,2),
    ingested_at TIMESTAMP,
    CONSTRAINT uq_product UNIQUE(product_id)
);


-- 4. CREATE SEQUENCE FOR SURROGATE KEY

CREATE SEQUENCE IF NOT EXISTS silver.product_key_seq;


-- 5. INCREMENTAL MERGE INTO DIMENSION

MERGE INTO silver.dim_products AS target
USING silver.stg_products AS source

ON target.product_id = source.product_id

WHEN NOT MATCHED THEN
INSERT (
    product_key,
    product_id,
    product_category_name,
    product_name_lenght,
    product_description_lenght,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    ingested_at  
)
VALUES (
    nextval('silver.product_key_seq'),
    source.product_id,
    source.product_category_name,
    source.product_name_lenght,
    source.product_description_lenght,
    source.product_photos_qty,
    source.product_weight_g,
    source.product_length_cm,
    source.product_height_cm,
    source.product_width_cm,
    source.ingested_at  
);


-- 6. INDEXES (IDEMPOTENT)

CREATE INDEX IF NOT EXISTS idx_stg_products_ingested
ON silver.stg_products(ingested_at);

CREATE INDEX IF NOT EXISTS idx_dim_products
ON silver.dim_products(product_id);