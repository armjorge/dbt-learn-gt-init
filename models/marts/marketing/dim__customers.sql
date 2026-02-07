with customers as (

     select * from {{ ref('stg_jaffle_shop__customers') }}

),

orders as ( 

    select * from {{ ref('stg_jaffle_shop__orders') }}

),

customer_orders as (

    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders

    from orders

    group by 1

),

payments as ( 
    SELECT 
    order_id
    , sum(amount) as amount
    FROM {{ref('stg_stripe__payments')}}
    WHERE status != 'fail'
    GROUP BY 1
),

payments_customer as (
    SELECT 
    customer_id

    , sum(amount) as amount
    FROM orders
    LEFT JOIN payments using(order_id)
    group by 1
),

final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce (customer_orders.number_of_orders, 0) as number_of_orders,
        coalesce (payments_customer.amount,0) as lifetime_value
        

    from customers

    left join customer_orders using (customer_id)
    left join payments_customer using (customer_id)

)

select * from final