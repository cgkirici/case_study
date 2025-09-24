with order_headers as (

    select *
    from {{ ref('stg__order_headers') }}

),

order_details as (

    select *
    from {{ ref('stg__order_details') }}

),

users as (

    select *
    from {{ ref('stg__users') }}

),

products as (

    select *
    from {{ ref('stg__products') }}

),

final as (

    select
        order_details.order_id,
        order_details.order_detail_id,
        order_details.product_id,
        order_details.order_date,
        order_details.quantity,
        order_details.net_price,
        order_details.net_price * order_details.quantity as net_amount,

        order_headers.user_id,
        order_headers.branch_id,

        products.product_name,
        products.product_category,

        users.created_at as user_created_date,

        case
            when users.deleted_at is null then FALSE
            else TRUE
        end is_user_deleted

    from order_details
    left join order_headers on order_details.order_id = order_headers.order_id
    left join users on order_headers.user_id = users.user_id
    left join products on order_details.product_id = products.product_id


)

select *
from final
