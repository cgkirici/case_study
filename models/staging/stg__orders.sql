with source as (

    select *
    from {{ source('case_study', 'orders_raw') }}

),

final as (

    select
        id as order_id,
        user_id,
        branch_id,
        parse_date('%m/%d/%Y', order_date) AS order_date,
        net_amount_total

    from source

)

select *
from final
