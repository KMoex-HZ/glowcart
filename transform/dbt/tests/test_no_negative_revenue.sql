-- Test: revenue must never be negative
-- Fails if any row has negative total_amount in fct_revenue
SELECT *
FROM {{ ref('fct_revenue') }}
WHERE total_revenue < 0
