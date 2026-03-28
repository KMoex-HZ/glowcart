
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Test: checkout count must never exceed add_to_cart count
-- Fails if funnel ordering is violated
SELECT *
FROM "glowcart"."main"."fct_funnel"
WHERE checkouts > add_to_carts
  
  
      
    ) dbt_internal_test