-- TODO: This query will return a table with the top 10 least revenue categories 
-- in English, the number of orders and their total revenue. The first column 
-- will be Category, that will contain the top 10 least revenue categories; the 
-- second one will be Num_order, with the total amount of orders of each 
-- category; and the last one will be Revenue, with the total revenue of each 
-- catgory.
-- HINT: All orders should have a delivered status and the Category and actual 
-- delivery date should be not null.

SELECT
    category_translation.product_category_name_english AS Category,
    COUNT(DISTINCT orders.order_id) AS Num_orders,
    SUM(order_payments.payment_value) AS Revenue
FROM
    olist_orders orders
        INNER JOIN
    olist_order_items order_items
    ON
            orders.order_id = order_items.order_id
        INNER JOIN
    olist_products products
    ON
            order_items.product_id = products.product_id
        INNER JOIN
    product_category_name_translation category_translation
    ON
            products.product_category_name = category_translation.product_category_name
        INNER JOIN
    olist_order_payments order_payments
    ON
            orders.order_id = order_payments.order_id
WHERE
        orders.order_status = 'delivered'
  AND orders.order_delivered_customer_date IS NOT NULL
  AND products.product_category_name IS NOT NULL
GROUP BY
    category_translation.product_category_name_english
ORDER BY
    Revenue ASC
LIMIT 10;














