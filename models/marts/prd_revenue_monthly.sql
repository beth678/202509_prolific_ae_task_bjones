with client_monthly as (
    select * from {{ ref('prd_client_revenue_monthly') }}
),

overall_gmv as (
    select 
        transaction_realised_month,
        sum(total_gmv) as total_gmv
    from 
        client_monthly
    group by transaction_realised_month
)

select * from overall_gmv