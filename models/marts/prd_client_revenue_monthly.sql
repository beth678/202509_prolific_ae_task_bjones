with client_transactions_daily as (
    select * from {{ref('prd_client_realised_transactions_daily')}}
),

client_monthly_metrics as (
    select 
        client_id,
        date(transaction_realised_date, 'start of month') as transaction_realised_month,
        sum(total_fees_charged_gbp) as total_fees_income_gbp, --unrealised chargebacks excluded
        sum(case when transaction_type = 'payment' then total_transaction_amount_gbp end) as total_gmv
    from client_transactions_daily
    group by client_id, date(transaction_realised_date, 'start of month')
)

select * from client_monthly_metrics