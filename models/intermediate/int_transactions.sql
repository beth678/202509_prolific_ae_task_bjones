--intermediate model converting to GBP
with transactions as (
    select * from {{ ref('stg_transactions') }}
),

currency_rates as (
    select * from {{ref('stg_currency_rates')}}
),

transaction_resolutions as (
    select * from {{ref('stg_transaction_resolutions')}}
),

--identify transaction realised date as transaction date for all transaction types 
--except chargebacks where transaction realised date is date of resolution or null
transaction_realised as (
    select
        t.transaction_id,
        t.client_id,
        t.transaction_amount,
        t.transaction_type,
        t.transaction_date,
        tr.resolution_status as chargeback_resolution_status,
        tr.resolution_date,
        --assume chargeback conversion happens on resolution date not transaction date, set as null if chargeback is not resolved as conversion date is not yet known
        case when t.transaction_type = 'chargeback' and tr.resolution_status = 'resolved' then tr.resolution_date
            when t.transaction_type = 'chargeback' and tr.resolution_status != 'resolved' then null
            else t.transaction_date end as transaction_realised_date,
        t.currency,
        t.platform_fee_margin,
        t.linked_transaction_id
    from    
        transactions t
    left join 
        transaction_resolutions tr on t.transaction_id = tr.transaction_id   
),

--convert transaction amounts to GBP based on conversion on transaction date
--exclude dates extending beyond available currency conversion data
transactions_gbp as (
    select
        t.transaction_id,
        t.client_id,
        t.transaction_amount as original_transaction_amount,
        t.transaction_amount * c.exchange_rate_to_gbp as transaction_amount_gbp,--note: if chargeback is not resolved, transaction_amount_gbp will be null
        t.transaction_type,
        t.chargeback_resolution_status,
        t.transaction_date,
        t.transaction_realised_date,
        t.platform_fee_margin,
        t.currency as original_currency,
        t.linked_transaction_id
    from    
        transaction_realised t
    left join 
        currency_rates c on t.currency = c.currency 
            and t.transaction_realised_date = c.rate_date
)

select * from transactions_gbp

