with src as (
    select * from {{ ref('transaction_resolutions_seed') }}
),

src_transaction_resolutions as (
    select
        transaction_id,
        resolution_status,
        --reformat date string to correctly parse date type
        date(
            substr(resolution_date, 7, 4) || '-' ||
            substr(resolution_date, 4, 2) || '-' ||
            substr(resolution_date, 1, 2)
        ) as resolution_date
    from src
)

select * from src_transaction_resolutions