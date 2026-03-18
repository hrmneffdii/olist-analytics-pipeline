# import library
import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text

# setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# create a db url in docker way
DB_URL = (
    f"postgresql://{os.getenv('POSTGRES_USER')}:"
    f"{os.getenv('POSTGRES_PASSWORD')}@postgres:5432/"
    f"{os.getenv('POSTGRES_DB')}"
)

# function to generate dim time
def generate_dim_time(start="2016-01-01", end="2025-12-31"):
    
    # create a dataframe dates
    dates = pd.date_range(start=start, end=end, freq="D")
    
    # dataframe created
    df = pd.DataFrame({
        "date_key":        dates.strftime("%Y%m%d").astype(int),
        "full_date":       dates.date,
        "day":             dates.day,
        "day_of_week":     dates.dayofweek + 1,        # 1=Senin, 7=Minggu
        "day_name":        dates.day_name(),
        "week_of_year":    dates.isocalendar().week.astype(int),
        "month":           dates.month,
        "month_name":      dates.month_name(),
        "quarter":         dates.quarter,
        "year":            dates.year,
        "is_weekend":      dates.dayofweek >= 5,
    })
    
    logger.info(f"Generated {len(df):,} rows for dim_time")
    
    # returning dataframe
    return df

# function that callable by apache airflow
def generate_dim_time_callable():
    
    # create an engine with db url path docker
    engine = create_engine(DB_URL)
    
    # receive dataframe 
    df = generate_dim_time()
    
    # create table dim time first if not exists
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS silver.dim_time (
        date_key INT PRIMARY KEY,
        full_date DATE NOT NULL,
        day INT NOT NULL,
        day_of_week INT NOT NULL,
        day_name VARCHAR(10) NOT NULL,
        week_of_year INT NOT NULL,
        month INT NOT NULL,
        month_name VARCHAR(10) NOT NULL,
        quarter INT NOT NULL,
        year INT NOT NULL,
        is_weekend BOOLEAN NOT NULL
    );
    """

    # execute sql command, create table first, then truncate to idempotent mode
    with engine.begin() as conn:
        conn.execute(text(create_table_sql))
        conn.execute(text("TRUNCATE TABLE silver.dim_time"))

    # append dataset into table
    df.to_sql(
        name="dim_time",
        schema="silver",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000
    )
    
    logger.info("dim_time loaded successfully ✓")
