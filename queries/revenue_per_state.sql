-- TODO: This query will return a table with two columns; customer_state, and 
-- Revenue. The first one will have the letters that identify the top 10 states 
-- with most revenue and the second one the total revenue of each.
-- HINT: All orders should have a delivered status and the actual delivery date 
-- should be not null. 

SELECT
    customers.customer_state AS customer_state,
    SUM(order_payments.payment_value) AS Revenue
FROM
    olist_orders orders
        INNER JOIN
    olist_customers customers
    ON
            orders.customer_id = customers.customer_id
        INNER JOIN
    olist_order_payments order_payments
    ON
            orders.order_id = order_payments.order_id
WHERE
        orders.order_status = 'delivered'
  AND orders.order_delivered_customer_date IS NOT NULL
GROUP BY
    customers.customer_state
ORDER BY
    Revenue DESC
LIMIT 10;



