-- Analytical Queries for E-commerce Data Warehouse
-- Demonstrates business intelligence and performance optimization

-- ============================================================
-- SALES ANALYTICS
-- ============================================================

-- Top 10 products by revenue
SELECT 
    p.product_name,
    p.category,
    SUM(f.total_amount) AS total_revenue,
    SUM(f.quantity) AS units_sold,
    COUNT(DISTINCT f.customer_id) AS unique_customers
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY p.product_name, p.category
ORDER BY total_revenue DESC
LIMIT 10;


-- Revenue by category and month
SELECT 
    p.category,
    DATE_TRUNC('month', f.date) AS month,
    SUM(f.total_amount) AS monthly_revenue,
    COUNT(f.sale_id) AS transactions,
    AVG(f.total_amount) AS avg_transaction_value
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY p.category, DATE_TRUNC('month', f.date)
ORDER BY category, month;


-- ============================================================
-- CUSTOMER ANALYTICS
-- ============================================================

-- Customer lifetime value by segment
SELECT 
    c.segment,
    COUNT(DISTINCT c.customer_id) AS total_customers,
    SUM(f.total_amount) AS total_revenue,
    AVG(customer_revenue) AS avg_customer_ltv,
    MAX(customer_revenue) AS max_customer_ltv
FROM dim_customer c
LEFT JOIN (
    SELECT 
        customer_id,
        SUM(total_amount) AS customer_revenue
    FROM fact_sales
    GROUP BY customer_id
) f ON c.customer_id = f.customer_id
GROUP BY c.segment
ORDER BY avg_customer_ltv DESC;


-- Top 20 customers by revenue
SELECT 
    c.name,
    c.email,
    c.segment,
    c.country,
    COUNT(f.sale_id) AS total_purchases,
    SUM(f.total_amount) AS total_spent,
    AVG(f.total_amount) AS avg_order_value,
    MAX(f.date) AS last_purchase_date
FROM dim_customer c
JOIN fact_sales f ON c.customer_id = f.customer_id
GROUP BY c.customer_id, c.name, c.email, c.segment, c.country
ORDER BY total_spent DESC
LIMIT 20;


-- ============================================================
-- DISCOUNT & PRICING ANALYTICS
-- ============================================================

-- Discount effectiveness by category
SELECT 
    p.category,
    AVG(f.discount_percent) AS avg_discount_pct,
    SUM(f.discount_amount) AS total_discount_given,
    SUM(f.total_amount) AS revenue_after_discount,
    SUM(f.amount_before_discount) AS revenue_before_discount,
    COUNT(CASE WHEN f.discount_percent > 0 THEN 1 END) AS discounted_sales,
    COUNT(f.sale_id) AS total_sales,
    ROUND(100.0 * COUNT(CASE WHEN f.discount_percent > 0 THEN 1 END) / COUNT(f.sale_id), 2) AS discount_rate_pct
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY p.category
ORDER BY total_discount_given DESC;


-- ============================================================
-- TIME-BASED ANALYTICS
-- ============================================================

-- Day of week sales pattern
SELECT 
    TO_CHAR(date, 'Day') AS day_of_week,
    EXTRACT(DOW FROM date) AS day_num,
    COUNT(sale_id) AS total_sales,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_sale_amount
FROM fact_sales
GROUP BY TO_CHAR(date, 'Day'), EXTRACT(DOW FROM date)
ORDER BY day_num;


-- Year-over-year growth
WITH monthly_sales AS (
    SELECT 
        EXTRACT(YEAR FROM date) AS year,
        EXTRACT(MONTH FROM date) AS month,
        SUM(total_amount) AS revenue
    FROM fact_sales
    GROUP BY EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date)
)
SELECT 
    current.year,
    current.month,
    current.revenue AS current_revenue,
    previous.revenue AS previous_year_revenue,
    ROUND(100.0 * (current.revenue - previous.revenue) / previous.revenue, 2) AS yoy_growth_pct
FROM monthly_sales current
LEFT JOIN monthly_sales previous 
    ON current.month = previous.month 
    AND current.year = previous.year + 1
ORDER BY current.year, current.month;


-- ============================================================
-- COHORT ANALYSIS
-- ============================================================

-- Customer cohort retention (by registration month)
WITH customer_cohorts AS (
    SELECT 
        customer_id,
        DATE_TRUNC('month', registration_date) AS cohort_month
    FROM dim_customer
),
purchase_months AS (
    SELECT 
        f.customer_id,
        DATE_TRUNC('month', f.date) AS purchase_month,
        SUM(f.total_amount) AS month_revenue
    FROM fact_sales f
    GROUP BY f.customer_id, DATE_TRUNC('month', f.date)
)
SELECT 
    cc.cohort_month,
    pm.purchase_month,
    COUNT(DISTINCT pm.customer_id) AS active_customers,
    SUM(pm.month_revenue) AS cohort_revenue
FROM customer_cohorts cc
JOIN purchase_months pm ON cc.customer_id = pm.customer_id
GROUP BY cc.cohort_month, pm.purchase_month
ORDER BY cc.cohort_month, pm.purchase_month;


-- ============================================================
-- PRODUCT PERFORMANCE
-- ============================================================

-- Product performance scorecard
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    p.unit_price,
    p.rating,
    COUNT(f.sale_id) AS times_sold,
    SUM(f.quantity) AS total_units_sold,
    SUM(f.total_amount) AS total_revenue,
    AVG(f.discount_percent) AS avg_discount_pct,
    COUNT(DISTINCT f.customer_id) AS unique_buyers
FROM dim_product p
LEFT JOIN fact_sales f ON p.product_id = f.product_id
GROUP BY p.product_id, p.product_name, p.category, p.unit_price, p.rating
ORDER BY total_revenue DESC NULLS LAST;
