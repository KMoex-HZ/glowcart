-- Revenue Performance Mart
-- Purpose: Aggregates daily sales performance at the Product and Category level.
-- Grain: Date | Product Category | Product Name

with events as (
    -- Import validated staging events
    select * from {{ ref('stg_events') }}
),

revenue_metrics as (
    -- Calculate key financial metrics for successful transactions only
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
    group by 1, 2, 3
)

-- Final output ordered by highest impact (revenue)
select * from revenue_metrics
order by total_revenue desc