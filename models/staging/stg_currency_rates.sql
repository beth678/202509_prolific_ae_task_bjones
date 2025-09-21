with src as (
    select * from {{ ref('currency_rates_seed') }}
),

src_currency_rates as (
    select
        currency,
        date(rate_date) as rate_date,
        exchange_rate_to_gbp
    from src
)

select * from src_currency_rates