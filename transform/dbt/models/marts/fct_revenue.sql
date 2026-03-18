with events as (
    select * from {{ ref('stg_events') }}
),

revenue as (
    select
        date,
        product_category,
        product_name,
        count(*) as total_orders,
        sum(total_amount) as total_revenue,
        avg(total_amount) as avg_order_value,
        sum(quantity) as total_units_sold
    from events
    where event_type = 'payment_success'
    group by date, product_category, product_name
)

select * from revenue
order by total_revenue desc
