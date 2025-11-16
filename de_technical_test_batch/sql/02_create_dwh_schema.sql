CREATE TABLE IF NOT EXISTS dwh.dim_date (
    date_key              INT PRIMARY KEY,
    full_date             DATE NOT NULL,
    day_of_week           SMALLINT NOT NULL,
    day_name              VARCHAR(10) NOT NULL,
    day_of_month          SMALLINT NOT NULL,
    day_of_year           SMALLINT NOT NULL,
    month_of_year         SMALLINT NOT NULL,
    month_name            VARCHAR(10) NOT NULL,
    quarter_of_year       SMALLINT NOT NULL,
    year_num              SMALLINT NOT NULL,
    is_weekend            BOOLEAN NOT NULL,
    is_holiday            BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS dwh.dim_customer (
    customer_key          BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    customer_id_natural   INT NOT NULL UNIQUE,
    name                  VARCHAR(255) NOT NULL,
    email                 VARCHAR(255),
    city                  VARCHAR(100),
    signup_date           TIMESTAMP,
    row_start_date        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    row_end_date          TIMESTAMP,
    is_current            BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS dwh.dim_product (
    product_key           BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    product_id_natural    INT NOT NULL UNIQUE,
    product_name          VARCHAR(255),
    category              VARCHAR(100),
    master_price          DECIMAL(18, 2)
);

CREATE TABLE IF NOT EXISTS dwh.dim_campaign (
    campaign_key          BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    campaign_id_natural   INT NOT NULL UNIQUE,
    campaign_name         VARCHAR(255),
    start_date            TIMESTAMP,
    end_date              TIMESTAMP,
    channel               VARCHAR(50)
);

INSERT INTO dwh.dim_customer (customer_key, customer_id_natural, name) 
OVERRIDING SYSTEM VALUE 
VALUES (0, 0, 'Unknown') 
ON CONFLICT (customer_key) DO NOTHING;

INSERT INTO dwh.dim_product (product_key, product_id_natural, product_name) 
OVERRIDING SYSTEM VALUE 
VALUES (0, 0, 'Unknown') 
ON CONFLICT (product_key) DO NOTHING;

INSERT INTO dwh.dim_campaign (campaign_key, campaign_id_natural, campaign_name) 
OVERRIDING SYSTEM VALUE 
VALUES (0, 0, 'Unknown') 
ON CONFLICT (campaign_key) DO NOTHING;

CREATE TABLE IF NOT EXISTS dwh.fact_sales (
    date_key              INT NOT NULL,
    customer_key          BIGINT NOT NULL,
    product_key           BIGINT NOT NULL,
    campaign_key          BIGINT NOT NULL DEFAULT 0,
    transaction_id_natural INT NOT NULL,
    quantity              INT NOT NULL,
    price_at_sale         DECIMAL(18, 2) NOT NULL,
    total_amount          DECIMAL(18, 2) NOT NULL,
    CONSTRAINT fk_date FOREIGN KEY (date_key) REFERENCES dwh.dim_date(date_key),
    CONSTRAINT fk_customer FOREIGN KEY (customer_key) REFERENCES dwh.dim_customer(customer_key),
    CONSTRAINT fk_product FOREIGN KEY (product_key) REFERENCES dwh.dim_product(product_key),
    CONSTRAINT fk_campaign FOREIGN KEY (campaign_key) REFERENCES dwh.dim_campaign(campaign_key)
);

CREATE INDEX IF NOT EXISTS idx_fact_date ON dwh.fact_sales(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_customer ON dwh.fact_sales(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_product ON dwh.fact_sales(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_campaign ON dwh.fact_sales(campaign_key);