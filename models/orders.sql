{{ config(schema='transaction') }}


WITH  
    sales AS (SELECT * FROM {{ref("sales")}})
    , orders AS (SELECT * FROM {{ref("stg_ship")}})
    -- AGGREGATE SALES BY ORDERS
    , aggreg_orders AS (
    SELECT 
        date_date
        ,orders_id
        ,count(products_id) AS count_products
        ,sum(qty) as total_products_in_orders
        ,ROUND(sum(turnover),2) as sum_turnover
        ,ROUND(sum(purchase_cost),2) as total_purchase_cost
        ,ROUND(SUM(margin) / NULLIF(SUM(turnover), 0) * 100,2) as margin_percent_per_order
        ,ROUND(sum(margin),2) AS total_margin

    FROM sales
    GROUP BY
        date_date
        ,orders_id
    )
-- IMPORT STG_SHIP
    , stg_ship AS (SELECT * FROM {{ref("stg_ship")}})
-- JOIN function --
    , ship_orders_join AS
    (
    SELECT 

        a.date_date,
        a.orders_id,
        a.count_products,
        a.total_products_in_orders,
        a.sum_turnover,
        a.total_purchase_cost,
        a.margin_percent_per_order,
        a.total_margin,
        b.shipping_fee,
        b.log_cost,
        b.ship_cost


    FROM aggreg_orders a
    LEFT JOIN stg_ship b ON a.orders_id = b.orders_id
    )
-- Enrichment-- 
SELECT *
,ROUND(total_margin + shipping_fee - ship_cost - log_cost,2) AS operational_margin 

FROM ship_orders_join
