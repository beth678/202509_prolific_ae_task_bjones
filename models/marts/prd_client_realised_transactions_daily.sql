with client_transactions as (
    select * from {{ref('int_client_transactions')}}
),

client_transactions_daily as (
    select
        client_id,
        contract_id,
        spend_threshold,
        transaction_realised_date,
        transaction_type,
        spend_threshold_reached,
        sum(transaction_amount_gbp) as total_transaction_amount_gbp,
        sum(fees_charged) as total_fees_charged_gbp
    from    
        client_transactions
    where transaction_type != 'chargeback' or (transaction_type = 'chargeback' and chargeback_resolution_status = 'resolved')
    group by client_id,
        contract_id,
        spend_threshold,
        transaction_realised_date,
        transaction_type,
        spend_threshold_reached
),

--note duplication of cumulative sum, but required as fee charged margin is set at the transaction level.
client_transactions_cumulative as (
    select
        client_id,
        contract_id,
        spend_threshold,
        transaction_realised_date,
        transaction_type,
        spend_threshold_reached,
        total_transaction_amount_gbp,
        total_fees_charged_gbp,
        sum(case when contract_id is not null and transaction_type = 'payment' then total_transaction_amount_gbp end) over (
            partition by contract_id 
            order by transaction_realised_date asc 
            rows between unbounded preceding and current row
        ) as cumulative_daily_contract_spend_gbp 
    from
       client_transactions_daily 
)

select * from client_transactions_cumulative
    
---Use for spend threshold tracking & discount application status - visualise by limiting to payments only