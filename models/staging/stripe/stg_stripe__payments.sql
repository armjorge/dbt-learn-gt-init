
SELECT 
    id as payment_id,
    ORDERID as order_id,
    paymentmethod, 
    status, 
    amount / 100 as amount,
    created as created_at,
    _batched_at
FROM {{source('payment', 'payment')}}