WITH customer_orders AS (
    SELECT
        customer_id,
        COUNT(DISTINCT date_id) AS order_count,
        SUM(payment_amount) AS total_revenue,
        MIN(date_id) AS first_order_date,
        DATE_PART('week', date_id) AS period_id
    FROM f_sales
    GROUP BY customer_id, DATEPART(WEEK, date_id)
),
new_customers AS (
    SELECT 
        period_id,
        COUNT(customer_id) AS new_customers_count,
        SUM(total_revenue) AS new_customers_revenue
    FROM customer_orders
    WHERE order_count = 1
    GROUP BY period_id
),
returning_customers AS (
    SELECT 
        period_id,
        COUNT(customer_id) AS returning_customers_count,
        SUM(total_revenue) AS returning_customers_revenue
    FROM customer_orders
    WHERE order_count > 1
    GROUP BY period_id
),
refunded_customers AS (
    SELECT 
        DATE_PART('week', date_id) AS period_id,
        COUNT(DISTINCT customer_id) AS refunded_customer_count,
        COUNT(*) AS customers_refunded
    FROM f_sales
    WHERE payment_amount < 0  
    GROUP BY DATE_PART('week', date_id)
)
INSERT INTO mart.f_customer_retention (
    period_name, period_id, item_id,
    new_customers_count, returning_customers_count, refunded_customer_count,
    new_customers_revenue, returning_customers_revenue, customers_refunded
)
SELECT 
    'weekly' AS period_name,
    nc.period_id,
    fds.item_id,
    nc.new_customers_count,
    rc.returning_customers_count,
    rfc.refunded_customer_count,
    nc.new_customers_revenue,
    rc.returning_customers_revenue,
    rfc.customers_refunded
FROM new_customers nc
LEFT JOIN returning_customers rc ON nc.period_id = rc.period_id
LEFT JOIN refunded_customers rfc ON nc.period_id = rfc.period_id
LEFT JOIN f_sales fds ON nc.period_id = DATE_PART('week', fds.date_id)
GROUP BY nc.period_id, fds.item_id, 
         nc.new_customers_count, rc.returning_customers_count, rfc.refunded_customer_count, 
         nc.new_customers_revenue, rc.returning_customers_revenue, rfc.customers_refunded;
