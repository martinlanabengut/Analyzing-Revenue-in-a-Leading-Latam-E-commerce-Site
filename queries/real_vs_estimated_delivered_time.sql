-- TODO: This query will return a table with the differences between the real 
-- and estimated delivery times by month and year. It will have different 
-- columns: month_no, with the month numbers going from 01 to 12; month, with 
-- the 3 first letters of each month (e.g. Jan, Feb); Year2016_real_time, with 
-- the average delivery time per month of 2016 (NaN if it doesn't exist); 
-- Year2017_real_time, with the average delivery time per month of 2017 (NaN if 
-- it doesn't exist); Year2018_real_time, with the average delivery time per 
-- month of 2018 (NaN if it doesn't exist); Year2016_estimated_time, with the 
-- average estimated delivery time per month of 2016 (NaN if it doesn't exist); 
-- Year2017_estimated_time, with the average estimated delivery time per month 
-- of 2017 (NaN if it doesn't exist) and Year2018_estimated_time, with the 
-- average estimated delivery time per month of 2018 (NaN if it doesn't exist).
-- HINTS
-- 1. You can use the julianday function to convert a date to a number.
-- 2. order_status == 'delivered' AND order_delivered_customer_date IS NOT NULL
-- 3. Take distinct order_id.

WITH Dates AS (
    SELECT
        strftime('%m', order_purchase_timestamp) AS month_no,
        strftime('%Y', order_purchase_timestamp) AS year,
        AVG(julianday(order_delivered_customer_date) - julianday(order_purchase_timestamp)) AS real_time,
        AVG(julianday(order_estimated_delivery_date) - julianday(order_purchase_timestamp)) AS estimated_time
    FROM
        olist_orders
    WHERE
            order_status = 'delivered'
      AND order_delivered_customer_date IS NOT NULL
    GROUP BY
        strftime('%m', order_purchase_timestamp),
        strftime('%Y', order_purchase_timestamp)
),
     MonthNames AS (
         SELECT '01' AS month_no, 'Jan' AS month UNION ALL
         SELECT '02', 'Feb' UNION ALL SELECT '03', 'Mar' UNION ALL SELECT '04', 'Apr' UNION ALL
         SELECT '05', 'May' UNION ALL SELECT '06', 'Jun' UNION ALL SELECT '07', 'Jul' UNION ALL
         SELECT '08', 'Aug' UNION ALL SELECT '09', 'Sep' UNION ALL SELECT '10', 'Oct' UNION ALL
         SELECT '11', 'Nov' UNION ALL SELECT '12', 'Dec'
     )
SELECT
    mn.month_no,
    mn.month,
    MAX(CASE WHEN d.year = '2016' THEN d.real_time END)                AS Year2016_real_time,
    MAX(CASE WHEN d.year = '2017' THEN d.real_time END)                AS Year2017_real_time,
    MAX(CASE WHEN d.year = '2018' THEN d.real_time END)                AS Year2018_real_time,
    MAX(CASE WHEN d.year = '2016' THEN d.estimated_time END)           AS Year2016_estimated_time,
    MAX(CASE WHEN d.year = '2017' THEN d.estimated_time END)           AS Year2017_estimated_time,
    MAX(CASE WHEN d.year = '2018' THEN d.estimated_time END) AS Year2018_estimated_time
FROM
    MonthNames mn
        LEFT JOIN
    Dates d ON mn.month_no = d.month_no
GROUP BY
    mn.month_no,
    mn.month
ORDER BY
    mn.month_no;

