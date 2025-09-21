with contracts as (
    select * from {{ref('stg_client_contracts')}}
),

contract_id as (
    select 
        client_id || contract_start_date as contract_id, --assume only one contract at once per client
        client_id,
        contract_start_date,
        date(
                date(contract_start_date, '+' || contract_duration_months || ' months'), --get date of contract start date + months
                'start of month','+1 month', '-1 day' -- find last day of contract end month
            ) as contract_end_date,
        contract_duration_months,
        spend_threshold,
        discounted_fee_margin
    from    
        contracts
)

select * from contract_id