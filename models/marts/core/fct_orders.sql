with payments as (

    select * from {{ ref ('stg_payments') }}
),


orders as (

    select * from {{ ref ('stg_orders') }}

),

-- because some order_id first status could be fail and then after some time success
order_payments as (
    select
        order_id,
        sum(case when status = 'success' then amount end) as amount

    from payments
    group by 1
),

	

final as (
	select 
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        coalesce(order_payments.amount, 0) as amount
	from orders
	left join order_payments using (order_id)
	)
select * from final		