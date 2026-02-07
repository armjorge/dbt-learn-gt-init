WITH orders as (
    SELECT *
    FROM {{ref('stg_jaffle_shop__orders')}}
),
payments as ( 
    SELECT 
    *
    FROM {{ref('stg_stripe__payments')}}
),

orders_payments as (
    SELECT 
    order_id,
    sum (case when status = 'sucess' then amount end) as amount
    from payments 
    group by 1
),

final as (
    select 
    orders.order_id
    , orders.customer_id
    , coalesce (orders_payments.amount,0) as amount

    FROM orders
    LEFT JOIN orders_payments USING (order_id)
)

SELECT *
FROM final