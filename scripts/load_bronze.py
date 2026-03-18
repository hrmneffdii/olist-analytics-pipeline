# import library
import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text

# setup logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# create a db url in docker way
DB_URL = (
    f"postgresql://{os.getenv('POSTGRES_USER')}:"
    f"{os.getenv('POSTGRES_PASSWORD')}@postgres:5432/"
    f"{os.getenv('POSTGRES_DB')}"
)

# list of datasets which used
FILES = {
    "bronze.orders":         "olist_orders_dataset.csv",
    "bronze.order_items":    "olist_order_items_dataset.csv",
    "bronze.customers":      "olist_customers_dataset.csv",
    "bronze.products":       "olist_products_dataset.csv",
    "bronze.sellers":        "olist_sellers_dataset.csv",
    "bronze.order_payments": "olist_order_payments_dataset.csv",
    "bronze.order_reviews":  "olist_order_reviews_dataset.csv",
}

# path of datasets in docker
RAW_PATH = "/opt/airflow/data/raw"

# function to load csv file into postgres
def load_csv_to_postgres(engine, table_name, file_path):
    logger.info(f"Loading {file_path} → {table_name}")
    
    # read data first
    df = pd.read_csv(file_path)
    logger.info(f"  Rows read: {len(df):,}")
    
    # database always be truncated for cleaning first
    with engine.connect() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_name}"))
        conn.commit()
    
    # then, append data into the table
    df.to_sql(
        name=table_name.split(".")[1],   
        schema=table_name.split(".")[0], 
        con=engine,
        if_exists="append",              
        index=False,
        method="multi",                  
        chunksize=1000
    )
    
    # ensure that the data succesfully inserted
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
        count = result.scalar()
    
    logger.info(f"  Rows loaded: {count:,} ✓")

# function to load all csv as well as callable by apache airflow
def load_bronze_callable():
    
    # create sqlalchemy engine with db url path in docker way
    engine = create_engine(DB_URL)
    logger.info("Connected to PostgreSQL")
    
    # for loop all csv file
    for table_name, filename in FILES.items():
        file_path = os.path.join(RAW_PATH, filename)
        if not os.path.exists(file_path):
            logger.warning(f"File not found: {file_path}, skipping...")
            continue
        load_csv_to_postgres(engine, table_name, file_path)
    
    logger.info("All files loaded successfully ✓")
