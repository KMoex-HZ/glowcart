-- Downstream Transformation: Example Filter
-- Purpose: Demonstrates the 'ref' function by creating a dependency 
-- on the upstream 'my_first_dbt_model'.

select *
from {{ ref('my_first_dbt_model') }}
where id = 1