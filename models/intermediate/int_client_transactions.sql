with transactions as (
    select * from {{ ref('int_transactions') }}
),

clients as (
    select * from {{ ref('int_client_contracts')}}
),

client_transactions as (
    select  
        t.client_id,
        c.contract_id,
        c.spend_threshold,
        c.discounted_fee_margin,
        t.original_transaction_amount,
        t.transaction_amount_gbp,
        t.platform_fee_margin,
        t.transaction_date,
        t.transaction_realised_date,
        t.transaction_type,
        t.chargeback_resolution_status,
        sum(case when contract_id is not null and transaction_type = 'payment' then t.transaction_amount_gbp end) over (
            partition by c.contract_id 
            order by t.transaction_date asc 
            rows between unbounded preceding and current row
        ) as cumulative_contract_spend_gbp --only payments considered and only populated for clients with contracts
    from
        transactions t
    left join
        clients c on 
            t.client_id = c.client_id 
            and t.transaction_date >= c.contract_start_date 
            and t.transaction_date <= c.contract_end_date----join transaction datebetween contract dates (as some clients have transactions before contract start)
),

contract_spend_threshold_reached_date as (
    select
        contract_id,
        min(transaction_date) as spend_threshold_reached_date
    from
        client_transactions
    where 
        cumulative_contract_spend_gbp > spend_threshold
    group by contract_id
),

client_spend_threshold as (
    select
        t.client_id,
        t.contract_id,
        t.spend_threshold,
        t.discounted_fee_margin,
        --t.original_transaction_amount,
        t.transaction_amount_gbp,
        t.platform_fee_margin,
        t.transaction_date,
        t.transaction_realised_date,
        t.transaction_type,
        t.cumulative_contract_spend_gbp,
        t.chargeback_resolution_status,
        case when t.contract_id is not null and t.transaction_date > spend_threshold_reached_date then 'Y'
            when t.contract_id is not null and t.transaction_date <= spend_threshold_reached_date then 'N'
            else null end as spend_threshold_reached,
        case when t.transaction_date > spend_threshold_reached_date then transaction_amount_gbp * discounted_fee_margin
            else transaction_amount_gbp * platform_fee_margin end as fees_charged    
    from
        client_transactions t 
    left join contract_spend_threshold_reached_date d on t.contract_id = d.contract_id
)

select 
    * 
from client_spend_threshold order by client_id, transaction_date asc

---TODO: EXCLUDE INCOMPLETE MONTHS
---TODO: WHAT IF A CLIENT HAS MULTIPLE CONTRACTS - need to introduce a contract number and secment by that
--WHAT TO DO ABOUT CLIENTS WITHOUT CONTRACTS?