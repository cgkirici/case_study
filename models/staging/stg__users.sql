with source as (

    select *
    from {{ source('case_study', 'users_raw') }}

),

final as (

    select
        id as user_id,
        parse_date('%m/%d/%Y', created_at) AS created_at,
        parse_date('%m/%d/%Y', deleted_at) AS deleted_at

    from source

)

select *
from final
