with source as (
    select * from read_parquet('/root/glowcart/storage/silver/events/date=*/events.parquet')
),

renamed as (
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
