CREATE TABLE IF NOT EXISTS staging.stg_customers (
    customer_id BIGINT PRIMARY KEY,
    name VARCHAR(255),
    email VARCHAR(255),
    city VARCHAR(100),
    signup_date TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.stg_products (
    product_id BIGINT PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(100),
    price DECIMAL(18, 2)
);

CREATE TABLE IF NOT EXISTS staging.stg_marketing_campaigns (
    campaign_id BIGINT PRIMARY KEY,
    campaign_name VARCHAR(255),
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    channel VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS staging.stg_transactions (
    transaction_id BIGINT PRIMARY KEY,
    customer_id BIGINT,
    transaction_date TIMESTAMP,
    total_amount DECIMAL(18, 2)
);

CREATE TABLE IF NOT EXISTS staging.stg_transaction_items (
    transaction_item_id BIGINT PRIMARY KEY,
    transaction_id BIGINT,
    product_id BIGINT,
    quantity INT,
    price DECIMAL(18, 2)
);