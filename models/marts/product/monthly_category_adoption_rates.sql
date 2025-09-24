-- broader category trends
with rev as (

    select *
    from {{ ref('revenue') }}

),

monthly_totals as (

    select 
        date_trunc(order_date, month) as month,
        count(distinct user_id) as total_active_customers

    from rev
    group by date_trunc(order_date, month)

),

category_adoption as (

    select 
            product_category,
            date_trunc(order_date, month) as month,
            count(distinct user_id) as category_customers
            
    from rev
    group by product_category, date_trunc(order_date, month)

),

final as (

    select 
        ca.product_category,
        ca.month,
        ca.category_customers,
        mt.total_active_customers,
        round((ca.category_customers / mt.total_active_customers), 2) as category_adoption_rate

    from category_adoption ca
    join monthly_totals mt on ca.month = mt.month
    order by ca.month desc, category_adoption_rate desc

)

select *
from final
