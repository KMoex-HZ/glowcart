-- Test: checkout count must never exceed add_to_cart count
-- Fails if funnel ordering is violated
SELECT *
FROM {{ ref('fct_funnel') }}
WHERE checkouts > add_to_carts
