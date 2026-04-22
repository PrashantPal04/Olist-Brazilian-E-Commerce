-- ============================================================
-- E-COMMERCE SALES ANALYTICS — COMPLETE SQL FILE
-- Dataset: Olist Brazilian E-Commerce
-- Database: PostgreSQL
-- ============================================================
-- HOW TO USE:
-- 1. Database + Tables ready karo (Section 0)
-- 2. CSV data load karo (Section 1)
-- 3. Baaki sections seedha run karo
-- Table names change karne ho toh:
--   fact_sales → tumhari fact table
--   dim_customers → tumhari customers table
-- ============================================================


-- ============================================================
-- SECTION 0: SCHEMA SETUP — STAR SCHEMA
-- ============================================================

CREATE TABLE dim_customers (
    customer_key        SERIAL PRIMARY KEY,
    customer_id         VARCHAR(50),
    customer_unique_id  VARCHAR(50),
    zip_code            VARCHAR(20),
    city                VARCHAR(100),
    state               VARCHAR(50)
);

CREATE TABLE dim_products (
    product_key         SERIAL PRIMARY KEY,
    product_id          VARCHAR(50),
    category_english    VARCHAR(100)
);

CREATE TABLE dim_time (
    time_key            INT PRIMARY KEY,
    full_date           DATE,
    day                 INT,
    month               INT,
    month_name          VARCHAR(20),
    quarter             INT,
    year                INT,
    is_weekend          BOOLEAN
);

CREATE TABLE dim_geography (
    geography_key       SERIAL PRIMARY KEY,
    zip_code            VARCHAR(20),
    city                VARCHAR(100),
    state               VARCHAR(50)
);

CREATE TABLE fact_sales (
    sale_id             SERIAL PRIMARY KEY,
    customer_key        INT REFERENCES dim_customers(customer_key),
    product_key         INT REFERENCES dim_products(product_key),
    geography_key       INT REFERENCES dim_geography(geography_key),
    time_key            INT REFERENCES dim_time(time_key),
    price               DECIMAL(10,2),
    freight_value       DECIMAL(10,2),
    total_amount        DECIMAL(10,2),
    order_status        VARCHAR(20),
    customer_unique_id  VARCHAR(50)
);


-- ============================================================
-- SECTION 1: ETL — DATA LOAD
-- ============================================================

-- Step 1: Staging tables banao
CREATE TABLE temp_products (
    product_id              VARCHAR(50),
    category_portuguese     VARCHAR(100),
    name_length             INT,
    description_length      INT,
    photos_qty              INT,
    weight_g                INT,
    length_cm               INT,
    height_cm               INT,
    width_cm                INT
);

CREATE TABLE temp_category (
    category_portuguese     VARCHAR(100),
    category_english        VARCHAR(100)
);

CREATE TABLE temp_geo (
    zip_code    VARCHAR(20),
    lat         DECIMAL(9,6),
    lng         DECIMAL(9,6),
    city        VARCHAR(100),
    state       VARCHAR(50)
);

CREATE TABLE temp_orders (
    order_id                VARCHAR(50),
    customer_id             VARCHAR(50),
    order_status            VARCHAR(20),
    purchase_timestamp      TIMESTAMP,
    approved_at             TIMESTAMP,
    delivered_carrier_date  TIMESTAMP,
    delivered_customer_date TIMESTAMP,
    estimated_delivery_date TIMESTAMP
);

CREATE TABLE temp_order_items (
    order_id        VARCHAR(50),
    order_item_id   INT,
    product_id      VARCHAR(50),
    seller_id       VARCHAR(50),
    shipping_date   TIMESTAMP,
    price           DECIMAL(10,2),
    freight_value   DECIMAL(10,2)
);

-- Step 2: CSV load karo (path apna daalo)
COPY dim_customers(customer_id, customer_unique_id, zip_code, city, state)
FROM '/your/path/olist_customers_dataset.csv'
DELIMITER ',' CSV HEADER;

COPY temp_products
FROM '/your/path/olist_products_dataset.csv'
DELIMITER ',' CSV HEADER;

COPY temp_category(category_portuguese, category_english)
FROM '/your/path/product_category_name_translation.csv'
DELIMITER ',' CSV HEADER;

COPY temp_geo(zip_code, lat, lng, city, state)
FROM '/your/path/olist_geolocation_dataset.csv'
DELIMITER ',' CSV HEADER;

COPY temp_orders
FROM '/your/path/olist_orders_dataset.csv'
DELIMITER ',' CSV HEADER;

COPY temp_order_items
FROM '/your/path/olist_order_items_dataset.csv'
DELIMITER ',' CSV HEADER;

-- Step 3: dim_products mein load karo (JOIN se English category)
INSERT INTO dim_products(product_id, category_english)
SELECT p.product_id, c.category_english
FROM temp_products p
JOIN temp_category c ON p.category_portuguese = c.category_portuguese;

-- Step 4: dim_geography mein load karo (duplicates remove)
INSERT INTO dim_geography(zip_code, city, state)
SELECT zip_code, city, state FROM temp_geo;

DELETE FROM dim_geography
WHERE geography_key NOT IN (
    SELECT MIN(geography_key)
    FROM dim_geography
    GROUP BY zip_code
);

-- Step 5: dim_time generate karo
INSERT INTO dim_time(time_key, full_date, day, month, month_name, quarter, year, is_weekend)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INT,
    d,
    EXTRACT(DAY FROM d),
    EXTRACT(MONTH FROM d),
    TO_CHAR(d, 'Month'),
    EXTRACT(QUARTER FROM d),
    EXTRACT(YEAR FROM d),
    CASE WHEN EXTRACT(DOW FROM d) IN (0,6) THEN TRUE ELSE FALSE END
FROM GENERATE_SERIES('2016-01-01'::DATE, '2018-12-31'::DATE, '1 day'::INTERVAL) d;

-- Step 6: fact_sales mein load karo
INSERT INTO fact_sales(customer_key, product_key, geography_key, time_key, price, freight_value, total_amount, order_status, customer_unique_id)
SELECT
    c.customer_key,
    p.product_key,
    g.geography_key,
    TO_CHAR(o.purchase_timestamp, 'YYYYMMDD')::INT,
    oi.price,
    oi.freight_value,
    oi.price + oi.freight_value,
    o.order_status,
    c.customer_unique_id
FROM temp_order_items oi
JOIN temp_orders o       ON oi.order_id = o.order_id
JOIN dim_customers c     ON o.customer_id = c.customer_id
JOIN dim_products p      ON oi.product_id = p.product_id
JOIN dim_geography g     ON c.zip_code = g.zip_code
JOIN dim_time t          ON TO_CHAR(o.purchase_timestamp, 'YYYYMMDD')::INT = t.time_key;

-- Step 7: Cleanup
DROP TABLE temp_products;
DROP TABLE temp_category;
DROP TABLE temp_geo;
DROP TABLE temp_orders;
DROP TABLE temp_order_items;


-- ============================================================
-- SECTION 2: DATA QUALITY CHECKS
-- ============================================================

-- Total rows check
SELECT COUNT(*) FROM fact_sales;

-- Null check
SELECT COUNT(*) FROM fact_sales
WHERE customer_key IS NULL
   OR product_key IS NULL
   OR geography_key IS NULL
   OR time_key IS NULL;

-- Order status distribution
SELECT order_status, COUNT(*)
FROM fact_sales
GROUP BY order_status;

-- Date range check
SELECT MIN(full_date), MAX(full_date) FROM dim_time
WHERE time_key IN (SELECT time_key FROM fact_sales);


-- ============================================================
-- SECTION 3: EDA — EXPLORATORY DATA ANALYSIS
-- ============================================================

-- 3.1 Yearly Revenue Trend
SELECT
    t.year,
    COUNT(f.sale_id) AS total_orders,
    ROUND(SUM(f.total_amount)::NUMERIC, 2) AS total_revenue
FROM fact_sales f
JOIN dim_time t ON f.time_key = t.time_key
GROUP BY t.year
ORDER BY t.year;

-- 3.2 Monthly Revenue Trend (2017-2018)
SELECT
    t.year,
    t.month,
    t.month_name,
    COUNT(f.sale_id) AS total_orders,
    ROUND(SUM(f.total_amount)::NUMERIC, 2) AS revenue
FROM fact_sales f
JOIN dim_time t ON f.time_key = t.time_key
WHERE t.year IN (2017, 2018)
GROUP BY t.year, t.month, t.month_name
ORDER BY t.year, t.month;

-- 3.3 Top 10 States by Revenue
SELECT
    g.state,
    COUNT(f.sale_id) AS total_orders,
    ROUND(SUM(f.total_amount)::NUMERIC, 2) AS revenue
FROM fact_sales f
JOIN dim_geography g ON f.geography_key = g.geography_key
GROUP BY g.state
ORDER BY revenue DESC
LIMIT 10;

-- 3.4 State Revenue % Share
SELECT
    c.state,
    ROUND(SUM(f.total_amount)::NUMERIC, 2) AS state_revenue,
    ROUND((SUM(f.total_amount) / SUM(SUM(f.total_amount)) OVER()) * 100, 2) AS percentage_share
FROM fact_sales f
JOIN dim_customers c ON f.customer_key = c.customer_key
GROUP BY c.state
ORDER BY percentage_share DESC;

-- 3.5 Top 10 Categories by Revenue
SELECT
    p.category_english,
    COUNT(f.sale_id) AS total_orders,
    ROUND(SUM(f.total_amount)::NUMERIC, 2) AS revenue,
    ROUND(AVG(f.total_amount)::NUMERIC, 2) AS avg_order_value
FROM fact_sales f
JOIN dim_products p ON f.product_key = p.product_key
GROUP BY p.category_english
ORDER BY revenue DESC
LIMIT 10;

-- 3.6 Top 10 Categories by AOV
SELECT
    p.category_english,
    COUNT(f.sale_id) AS total_orders,
    ROUND(SUM(f.total_amount)::NUMERIC, 2) AS revenue,
    ROUND(AVG(f.total_amount)::NUMERIC, 2) AS avg_order_value
FROM fact_sales f
JOIN dim_products p ON f.product_key = p.product_key
GROUP BY p.category_english
ORDER BY avg_order_value DESC
LIMIT 10;

-- 3.7 Repeat vs One-time Customers
WITH customer_tags AS (
    SELECT
        customer_unique_id,
        CASE WHEN COUNT(sale_id) > 1 THEN 'Repeat' ELSE 'One-time' END AS status
    FROM fact_sales
    GROUP BY customer_unique_id
)
SELECT status, COUNT(*) AS customer_count
FROM customer_tags
GROUP BY status;


-- ============================================================
-- SECTION 4: KPI CALCULATIONS
-- ============================================================

-- 4.1 GMV by Year
SELECT
    t.year,
    ROUND(SUM(f.total_amount)::NUMERIC, 2) AS GMV
FROM fact_sales f
JOIN dim_time t ON f.time_key = t.time_key
WHERE t.year IN (2017, 2018)
GROUP BY t.year
ORDER BY t.year;

-- 4.2 Net Revenue (Delivered only)
SELECT
    t.year,
    ROUND(SUM(f.total_amount)::NUMERIC, 2) AS net_revenue
FROM fact_sales f
JOIN dim_time t ON f.time_key = t.time_key
WHERE f.order_status = 'delivered'
  AND t.year IN (2017, 2018)
GROUP BY t.year
ORDER BY t.year;

-- 4.3 Average LTV
SELECT ROUND(AVG(total_spent)::NUMERIC, 2) AS avg_ltv
FROM (
    SELECT
        customer_unique_id,
        SUM(total_amount) AS total_spent
    FROM fact_sales
    GROUP BY customer_unique_id
) sub;

-- 4.4 Overall Retention Rate
WITH customer_stats AS (
    SELECT
        customer_unique_id,
        COUNT(sale_id) AS order_count
    FROM fact_sales
    GROUP BY customer_unique_id
)
SELECT
    COUNT(CASE WHEN order_count > 1 THEN 1 END) AS repeat_customers,
    COUNT(*) AS total_unique_customers,
    ROUND((COUNT(CASE WHEN order_count > 1 THEN 1 END)::NUMERIC / COUNT(*)) * 100, 2) AS retention_rate
FROM customer_stats;

-- 4.5 Monthly Retention Trend
WITH CustomerFirstOrder AS (
    SELECT
        customer_unique_id,
        MIN(t.full_date) AS first_order_date
    FROM fact_sales f
    JOIN dim_time t ON f.time_key = t.time_key
    GROUP BY customer_unique_id
),
MonthlyOrders AS (
    SELECT
        t.year,
        t.month,
        f.customer_unique_id,
        MIN(t.full_date) AS order_date
    FROM fact_sales f
    JOIN dim_time t ON f.time_key = t.time_key
    GROUP BY t.year, t.month, f.customer_unique_id
)
SELECT
    m.year,
    m.month,
    COUNT(DISTINCT m.customer_unique_id) AS total_customers,
    COUNT(DISTINCT CASE WHEN m.order_date > cfo.first_order_date THEN m.customer_unique_id END) AS repeat_customers,
    ROUND(
        COUNT(DISTINCT CASE WHEN m.order_date > cfo.first_order_date THEN m.customer_unique_id END)::NUMERIC
        / COUNT(DISTINCT m.customer_unique_id) * 100, 2
    ) AS retention_percentage
FROM MonthlyOrders m
JOIN CustomerFirstOrder cfo ON m.customer_unique_id = cfo.customer_unique_id
GROUP BY m.year, m.month
ORDER BY m.year, m.month;

-- 4.6 Top 10 Customers by Spend
SELECT
    c.customer_unique_id,
    ROUND(SUM(f.total_amount)::NUMERIC, 2) AS total_spent
FROM fact_sales f
JOIN dim_customers c ON f.customer_key = c.customer_key
GROUP BY c.customer_unique_id
ORDER BY total_spent DESC
LIMIT 10;

-- 4.7 Category YoY Growth (2017 vs 2018)
SELECT
    p.category_english,
    t.year,
    COUNT(f.sale_id) AS total_orders,
    ROUND(SUM(f.total_amount)::NUMERIC, 2) AS revenue,
    ROUND(AVG(f.total_amount)::NUMERIC, 2) AS AOV
FROM fact_sales f
JOIN dim_products p ON f.product_key = p.product_key
JOIN dim_time t ON f.time_key = t.time_key
WHERE t.year IN (2017, 2018)
GROUP BY p.category_english, t.year
ORDER BY revenue DESC;

-- 4.8 November Black Friday Analysis
SELECT
    p.category_english,
    ROUND(SUM(f.total_amount)::NUMERIC, 2) AS revenue
FROM fact_sales f
JOIN dim_products p ON f.product_key = p.product_key
JOIN dim_time t ON f.time_key = t.time_key
WHERE t.month = 11
GROUP BY p.category_english
ORDER BY revenue DESC
LIMIT 10;


-- ============================================================
-- SECTION 5: RFM SEGMENTATION
-- ============================================================

-- 5.1 Full RFM with Segments
WITH customer_metrics AS (
    SELECT
        c.customer_unique_id,
        MAX(t.full_date) AS last_purchase_date,
        COUNT(f.sale_id) AS total_orders,
        ROUND(SUM(f.total_amount)::NUMERIC, 2) AS total_spend
    FROM fact_sales f
    JOIN dim_customers c ON f.customer_key = c.customer_key
    JOIN dim_time t ON f.time_key = t.time_key
    GROUP BY c.customer_unique_id
),
rfm_base AS (
    SELECT
        customer_unique_id,
        ('2018-09-03'::DATE - last_purchase_date) AS recency,
        total_orders AS frequency,
        total_spend AS monetary
    FROM customer_metrics
),
rfm_scores AS (
    SELECT
        customer_unique_id,
        recency,
        frequency,
        monetary,
        NTILE(5) OVER (ORDER BY recency DESC)   AS r_score,
        NTILE(5) OVER (ORDER BY frequency ASC)  AS f_score,
        NTILE(5) OVER (ORDER BY monetary ASC)   AS m_score
    FROM rfm_base
)
SELECT
    customer_unique_id,
    recency,
    frequency,
    monetary,
    r_score,
    f_score,
    m_score,
    CONCAT(r_score, f_score, m_score) AS rfm_score,
    CASE
        WHEN r_score = 5 AND f_score >= 4 THEN 'Champions'
        WHEN r_score >= 4 AND f_score >= 4 THEN 'Loyal Customers'
        WHEN r_score >= 4 AND f_score <= 2 THEN 'Recent/New Customers'
        WHEN r_score <= 2 AND f_score >= 4 THEN 'Can''t Lose Them'
        WHEN r_score <= 2 AND f_score <= 2 THEN 'Lost Customers'
        ELSE 'About to Sleep'
    END AS customer_segment
FROM rfm_scores
ORDER BY r_score DESC, f_score DESC;

-- 5.2 Segment Distribution
WITH customer_metrics AS (
    SELECT
        c.customer_unique_id,
        MAX(t.full_date) AS last_purchase_date,
        COUNT(f.sale_id) AS total_orders,
        ROUND(SUM(f.total_amount)::NUMERIC, 2) AS total_spend
    FROM fact_sales f
    JOIN dim_customers c ON f.customer_key = c.customer_key
    JOIN dim_time t ON f.time_key = t.time_key
    GROUP BY c.customer_unique_id
),
rfm_base AS (
    SELECT
        customer_unique_id,
        ('2018-09-03'::DATE - last_purchase_date) AS recency,
        total_orders AS frequency,
        total_spend AS monetary
    FROM customer_metrics
),
rfm_scores AS (
    SELECT
        customer_unique_id,
        NTILE(5) OVER (ORDER BY recency DESC)   AS r_score,
        NTILE(5) OVER (ORDER BY frequency ASC)  AS f_score,
        NTILE(5) OVER (ORDER BY monetary ASC)   AS m_score
    FROM rfm_base
),
rfm_segments AS (
    SELECT
        customer_unique_id,
        CASE
            WHEN r_score = 5 AND f_score >= 4 THEN 'Champions'
            WHEN r_score >= 4 AND f_score >= 4 THEN 'Loyal Customers'
            WHEN r_score >= 4 AND f_score <= 2 THEN 'Recent/New Customers'
            WHEN r_score <= 2 AND f_score >= 4 THEN 'Can''t Lose Them'
            WHEN r_score <= 2 AND f_score <= 2 THEN 'Lost Customers'
            ELSE 'About to Sleep'
        END AS customer_segment
    FROM rfm_scores
)
SELECT
    customer_segment,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*)::NUMERIC / SUM(COUNT(*)) OVER() * 100, 2) AS percentage
FROM rfm_segments
GROUP BY customer_segment
ORDER BY customer_count DESC;


-- ============================================================
-- SECTION 6: SAVED VIEWS (Re-use karo kabhi bhi)
-- ============================================================

CREATE VIEW v_gmv_by_year AS
SELECT t.year, ROUND(SUM(f.total_amount)::NUMERIC, 2) AS gmv
FROM fact_sales f
JOIN dim_time t ON f.time_key = t.time_key
GROUP BY t.year;

CREATE VIEW v_revenue_by_state AS
SELECT c.state,
    COUNT(f.sale_id) AS total_orders,
    ROUND(SUM(f.total_amount)::NUMERIC, 2) AS revenue,
    ROUND(SUM(f.total_amount) / SUM(SUM(f.total_amount)) OVER() * 100, 2) AS pct_share
FROM fact_sales f
JOIN dim_customers c ON f.customer_key = c.customer_key
GROUP BY c.state;

CREATE VIEW v_category_performance AS
SELECT p.category_english, t.year,
    COUNT(f.sale_id) AS total_orders,
    ROUND(SUM(f.total_amount)::NUMERIC, 2) AS revenue,
    ROUND(AVG(f.total_amount)::NUMERIC, 2) AS aov
FROM fact_sales f
JOIN dim_products p ON f.product_key = p.product_key
JOIN dim_time t ON f.time_key = t.time_key
GROUP BY p.category_english, t.year;

CREATE VIEW v_retention_monthly AS
WITH CustomerFirstOrder AS (
    SELECT customer_unique_id, MIN(t.full_date) AS first_order_date
    FROM fact_sales f
    JOIN dim_time t ON f.time_key = t.time_key
    GROUP BY customer_unique_id
),
MonthlyOrders AS (
    SELECT t.year, t.month, f.customer_unique_id, MIN(t.full_date) AS order_date
    FROM fact_sales f
    JOIN dim_time t ON f.time_key = t.time_key
    GROUP BY t.year, t.month, f.customer_unique_id
)
SELECT m.year, m.month,
    COUNT(DISTINCT m.customer_unique_id) AS total_customers,
    COUNT(DISTINCT CASE WHEN m.order_date > cfo.first_order_date THEN m.customer_unique_id END) AS repeat_customers,
    ROUND(COUNT(DISTINCT CASE WHEN m.order_date > cfo.first_order_date THEN m.customer_unique_id END)::NUMERIC / COUNT(DISTINCT m.customer_unique_id) * 100, 2) AS retention_pct
FROM MonthlyOrders m
JOIN CustomerFirstOrder cfo ON m.customer_unique_id = cfo.customer_unique_id
GROUP BY m.year, m.month;
