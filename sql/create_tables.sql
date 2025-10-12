-- Create Star Schema Tables for Redshift
-- Data Warehouse: E-commerce Analytics

-- ============================================================
-- DIMENSION TABLES
-- ============================================================

-- Customers Table
DROP TABLE IF EXISTS customers CASCADE;
CREATE TABLE customers (
    customer_id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    country VARCHAR(100),
    registration_date DATE,
    segment VARCHAR(50),
    city VARCHAR(255)
)
DISTSTYLE KEY
DISTKEY (customer_id)
SORTKEY (customer_id, registration_date);

COMMENT ON TABLE customers IS 'Customer dimension with demographic and segmentation data';


-- Products Table
DROP TABLE IF EXISTS products CASCADE;
CREATE TABLE products (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    supplier VARCHAR(255),
    rating DECIMAL(3,1)
)
DISTSTYLE ALL
SORTKEY (category, product_id);

COMMENT ON TABLE products IS 'Product dimension with catalog and supplier information';


-- ============================================================
-- FACT TABLE
-- ============================================================

-- Sales Fact Table
DROP TABLE IF EXISTS sales CASCADE;
CREATE TABLE sales (
    sale_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(customer_id),
    product_id INTEGER NOT NULL REFERENCES products(product_id),
    sale_date DATE NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10,2) NOT NULL,
    discount_percent DECIMAL(5,2) DEFAULT 0,
    amount_before_discount DECIMAL(12,2) NOT NULL,
    discount_amount DECIMAL(12,2) DEFAULT 0,
    total_amount DECIMAL(12,2) NOT NULL,
    payment_method VARCHAR(50),
    status VARCHAR(50)
)
DISTSTYLE KEY
DISTKEY (customer_id)
SORTKEY (sale_date, customer_id);

COMMENT ON TABLE sales IS 'Sales fact table with transaction details and calculated metrics';


-- ============================================================
-- INDEXES FOR QUERY OPTIMIZATION
-- ============================================================

-- Note: Redshift doesn't support traditional indexes
-- Instead, we use SORTKEY and DISTKEY for optimization
-- Additional indexes can be added in other databases

-- ============================================================
-- SUMMARY VIEWS
-- ============================================================

-- Sales by Category View
CREATE OR REPLACE VIEW vw_sales_by_category AS
SELECT
    p.category,
    COUNT(DISTINCT s.sale_id) AS total_sales,
    SUM(s.quantity) AS total_quantity,
    SUM(s.total_amount) AS total_revenue,
    AVG(s.total_amount) AS avg_sale_amount,
    SUM(s.discount_amount) AS total_discounts
FROM sales s
JOIN products p ON s.product_id = p.product_id
GROUP BY p.category;


-- Sales by Customer Segment View
CREATE OR REPLACE VIEW vw_sales_by_segment AS
SELECT
    c.segment,
    COUNT(DISTINCT s.customer_id) AS active_customers,
    COUNT(s.sale_id) AS total_sales,
    SUM(s.total_amount) AS total_revenue,
    AVG(s.total_amount) AS avg_sale_amount
FROM sales s
JOIN customers c ON s.customer_id = c.customer_id
GROUP BY c.segment;


-- Monthly Sales Trend View
CREATE OR REPLACE VIEW vw_monthly_sales AS
SELECT
    DATE_TRUNC('month', sale_date) AS month,
    COUNT(sale_id) AS total_sales,
    SUM(total_amount) AS monthly_revenue,
    AVG(total_amount) AS avg_sale_amount,
    SUM(discount_amount) AS total_discounts
FROM sales
GROUP BY DATE_TRUNC('month', sale_date)
ORDER BY month;
