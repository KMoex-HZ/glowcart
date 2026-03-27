-- Staging Model: GlowCart Events
-- Purpose: Ingests raw silver layer events from partitioned Parquet storage.
-- Optimization: Uses DuckDB's globbing pattern to scan all date-based partitions.

with source as (
    -- Direct ingestion from silver layer Parquet files
    select * from read_parquet('/root/glowcart/storage/silver/events/date=*/events.parquet')
),

renamed as (
    -- Schema definition for downstream marts
    -- Ensures consistent column names and data types across the platform
    select
        event_id,
        event_type,
        timestamp,
        session_id,
        device,
        platform,
        user_id,
        user_name,
        user_email,
        user_city,
        user_age,
        product_id,
        product_name,
        product_price,
        product_category,
        quantity,
        total_amount,
        is_transaction,
        hour,
        date
    from source
)

select * from renamed