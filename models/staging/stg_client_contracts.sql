with src as (
    select * from {{ ref('client_contracts_seed') }}
),

src_client_contracts as (
    select
        client_id,
        date(contract_start_date) as contract_start_date,
        contract_duration_months,
        spend_threshold,
        discounted_fee_margin
    from src
)

select * from src_client_contracts