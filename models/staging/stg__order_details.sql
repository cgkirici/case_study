with source as (

    select *
    from {{ source('case_study', 'order_details_raw') }}

),

final as (

    select
        id as order_detail_id,
        order_id,
        product_id,
        quantity,
        parse_date('%m/%d/%Y', order_date) AS order_date,
        net_price

    from source

)

select *
from final
