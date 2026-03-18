# import library
import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# import function from scripts
from scripts.load_bronze import load_bronze_callable
from scripts.generate_dim_date import generate_dim_time_callable

# DAG setup
with DAG(
    dag_id="olist",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),  
    schedule=None,                                        
    catchup=False,
    tags=["olist"],
    template_searchpath=["/opt/airflow/sql"],  
) as dag:

    # load bronze csv data to postgres database
    load_bronze = PythonOperator(
        task_id="load_bronze",
        python_callable=load_bronze_callable
    )
    
    # generate dim time to postgres
    generate_dim_time = PythonOperator(
        task_id="generate_dim_date",
        python_callable=generate_dim_time_callable
    )
    
    # create dim_customers
    load_dim_customers = SQLExecuteQueryOperator(
        task_id="load_dim_customers",
        conn_id="postgres_default",
        sql="silver/dim_customers.sql",  
    )
    
    # create dim_products
    load_dim_products = SQLExecuteQueryOperator(
        task_id="load_dim_products",
        conn_id="postgres_default",
        sql="silver/dim_products.sql",  
    )
    
    # create dim_sellers
    load_dim_sellers = SQLExecuteQueryOperator(
        task_id="load_dim_sellers",
        conn_id="postgres_default",
        sql="silver/dim_sellers.sql",  
    )

    # create fact_orders
    load_fact_orders = SQLExecuteQueryOperator(
        task_id="load_fact_orders",
        conn_id="postgres_default",
        sql="silver/fact_orders.sql",  
    )
    
    # run dbt for gold layer
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt run "
            "--profiles-dir /opt/airflow/dbt "
            "--log-path /tmp/dbt_logs"
        )
    )

    # dbt test to check last output
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt test "
            "--profiles-dir /opt/airflow/dbt "
            "--log-path /tmp/dbt_logs"
        )
    )

    # 1. load_bronze first, then generate dim_time
    load_bronze >> generate_dim_time
    
    # 2. generate dim_time, then create dims paralel as well as fact orders
    generate_dim_time >> [load_dim_customers, load_dim_sellers, load_dim_products] >> load_fact_orders
    
    # 3. run dbt test after fact_orders created
    load_fact_orders >> dbt_run >> dbt_test
        
    
