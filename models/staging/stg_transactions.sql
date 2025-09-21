with src as (
    select * from {{ ref('transactions_seed') }}
),

src_transactions as (
    select
        transaction_id,
        client_id,
        transaction_amount,
        transaction_type,
        date(transaction_date) as transaction_date,
        platform_fee_margin,
        currency,
        linked_transaction_id
    from src
)

select * from src_transactions