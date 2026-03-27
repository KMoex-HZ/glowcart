{{ config(materialized='table') }}

-- Testing Model: Materialization Verification
-- Purpose: Simple mock model to verify dbt's ability to create a physical table 
-- and handle nullable values in the warehouse.

with source_data as (
    -- Generating dummy records for testing purposes
    select 1 as id
    union all
    select null as id
)

select *
from source_data