with events as (
    select * from "glowcart"."main"."stg_events"
),

funnel as (
    select
        date,
        count(case when event_type = 'page_view' then 1 end) as page_views,
        count(case when event_type = 'add_to_cart' then 1 end) as add_to_carts,
        count(case when event_type = 'checkout' then 1 end) as checkouts,
        count(case when event_type = 'payment_success' then 1 end) as purchases,
        count(case when event_type = 'payment_failed' then 1 end) as failed_payments
    from events
    group by date
),

with_rates as (
    select
        date,
        page_views,
        add_to_carts,
        checkouts,
        purchases,
        failed_payments,
        round(add_to_carts * 100.0 / nullif(page_views, 0), 1) as cart_rate_pct,
        round(checkouts * 100.0 / nullif(add_to_carts, 0), 1) as checkout_rate_pct,
        round(purchases * 100.0 / nullif(checkouts, 0), 1) as purchase_rate_pct
    from funnel
)

select * from with_rates