with source as (

    select *
    from {{ source('case_study', 'products_raw') }}

),

final as (

    select
        id as product_id,
        name as product_name,
        category as product_category

    from source

)

select *
from final
