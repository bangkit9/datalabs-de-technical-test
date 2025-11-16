from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator
from pendulum import datetime
import logging

POSTGRES_CONN_ID = "postgres_staging_db"
STAGING_SCHEMA = "staging"
DWH_SCHEMA = "dwh"
DATE_DIM_START_YEAR = 2020
DATE_DIM_END_YEAR = 2030

@task
def load_dim_customers():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    sql = f"""
        INSERT INTO {DWH_SCHEMA}.dim_customer (
            customer_id_natural, name, email, city, signup_date
        )
        SELECT
            customer_id, name, email, city, signup_date
        FROM {STAGING_SCHEMA}.stg_customers
        ON CONFLICT (customer_id_natural) DO UPDATE
        SET
            name = EXCLUDED.name,
            email = EXCLUDED.email,
            city = EXCLUDED.city,
            is_current = TRUE,
            row_end_date = NULL;
    """
    hook.run(sql)
    logging.info("Successfully loaded dim_customer.")

@task
def load_dim_products():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    sql = f"""
        INSERT INTO {DWH_SCHEMA}.dim_product (
            product_id_natural, product_name, category, master_price
        )
        SELECT
            product_id, product_name, category, price
        FROM {STAGING_SCHEMA}.stg_products
        ON CONFLICT (product_id_natural) DO UPDATE
        SET
            product_name = EXCLUDED.product_name,
            category = EXCLUDED.category,
            master_price = EXCLUDED.master_price;
    """
    hook.run(sql)
    logging.info("Successfully loaded dim_product.")

@task
def load_dim_campaigns():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    sql = f"""
        INSERT INTO {DWH_SCHEMA}.dim_campaign (
            campaign_id_natural, campaign_name, start_date, end_date, channel
        )
        SELECT
            campaign_id, campaign_name, start_date, end_date, channel
        FROM {STAGING_SCHEMA}.stg_marketing_campaigns
        ON CONFLICT (campaign_id_natural) DO UPDATE
        SET
            campaign_name = EXCLUDED.campaign_name,
            start_date = EXCLUDED.start_date,
            end_date = EXCLUDED.end_date,
            channel = EXCLUDED.channel;
    """
    hook.run(sql)
    logging.info("Successfully loaded dim_campaign.")

@task
def populate_dim_date():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    sql = f"""
        INSERT INTO {DWH_SCHEMA}.dim_date (
            date_key,
            full_date,
            day_of_week,
            day_name,
            day_of_month,
            day_of_year,
            month_of_year,
            month_name,
            quarter_of_year,
            year_num,
            is_weekend,
            is_holiday
        )
        SELECT
            TO_CHAR(d, 'YYYYMMDD')::INT AS date_key,
            d::DATE AS full_date,
            EXTRACT(ISODOW FROM d) AS day_of_week,
            TO_CHAR(d, 'Day') AS day_name,
            EXTRACT(DAY FROM d) AS day_of_month,
            EXTRACT(DOY FROM d) AS day_of_year,
            EXTRACT(MONTH FROM d) AS month_of_year,
            TO_CHAR(d, 'Month') AS month_name,
            EXTRACT(QUARTER FROM d) AS quarter_of_year,
            EXTRACT(YEAR FROM d) AS year_num,
            EXTRACT(ISODOW FROM d) IN (6, 7) AS is_weekend,
            FALSE AS is_holiday
            
        FROM generate_series(
            '{DATE_DIM_START_YEAR}-01-01'::DATE,
            '{DATE_DIM_END_YEAR}-12-31'::DATE,
            '1 day'::INTERVAL
        ) AS t(d)
        
        ON CONFLICT (date_key) DO NOTHING;
    """
    hook.run(sql)
    logging.info(f"dim_date is populated/verified for {DATE_DIM_START_YEAR}-{DATE_DIM_END_YEAR}.")

@task
def load_fact_sales():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    truncate_sql = f"TRUNCATE TABLE {DWH_SCHEMA}.fact_sales;"
    
    insert_sql = f"""
        INSERT INTO {DWH_SCHEMA}.fact_sales (
            date_key,
            customer_key,
            product_key,
            campaign_key,
            transaction_id_natural,
            quantity,
            price_at_sale,
            total_amount
        )
        SELECT
            TO_CHAR(t.transaction_date, 'YYYYMMDD')::INT AS date_key,
            COALESCE(dc.customer_key, 0) AS customer_key,
            COALESCE(dp.product_key, 0) AS product_key,
            0 AS campaign_key, 
            t.transaction_id AS transaction_id_natural,
            ti.quantity,
            ti.price AS price_at_sale,
            (ti.quantity * ti.price) AS total_amount

        FROM {STAGING_SCHEMA}.stg_transactions t
        
        JOIN {STAGING_SCHEMA}.stg_transaction_items ti
            ON t.transaction_id = ti.transaction_id
            
        LEFT JOIN {DWH_SCHEMA}.dim_customer dc
            ON t.customer_id = dc.customer_id_natural AND dc.is_current = TRUE
            
        LEFT JOIN {DWH_SCHEMA}.dim_product dp
            ON ti.product_id = dp.product_id_natural
            
        WHERE t.transaction_date >= '{DATE_DIM_START_YEAR}-01-01'
          AND t.transaction_date <= '{DATE_DIM_END_YEAR}-12-31';
    """
    logging.info("Truncating fact_sales for idempotent load...")
    hook.run(truncate_sql)
    logging.info("Loading new data into fact_sales...")
    hook.run(insert_sql)
    logging.info("Successfully loaded fact_sales.")


@dag(
    dag_id="task_2_transform_to_dwh",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=['etl', 'transform', 'dwh']
)
def transform_to_dwh_dag():
    start = EmptyOperator(task_id="start")
    
    load_dims = [
        load_dim_customers(),
        load_dim_products(),
        load_dim_campaigns(),
        populate_dim_date()
    ]
    
    load_fact = load_fact_sales()

    end = EmptyOperator(task_id="end")

    start >> load_dims >> load_fact >> end

transform_to_dwh_dag()