
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Test: revenue must never be negative
-- Fails if any row has negative total_amount in fct_revenue
SELECT *
FROM "glowcart"."main"."fct_revenue"
WHERE total_revenue < 0
  
  
      
    ) dbt_internal_test