-- adoption momentum score with product-level detail
with rev as (

    select *
    from {{ ref('revenue') }}

),

monthly_active_customers as (

    select
        date_trunc(order_date, month) as month,
        count(distinct user_id) as total_active_customers

    from rev
    group by date_trunc(order_date, month)

),

product_monthly_adoption as (

    select 
        r.product_id,
        r.product_name,
        r.product_category,
        date_trunc(r.order_date, month) as month,
        count(distinct r.user_id) as product_customers

    from `circ.revenue` r
    group by r.product_id, r.product_name, r.product_category, date_trunc(r.order_date, month)

),

monthly_adoption as (

    select 
        pma.product_id,
        pma.product_name,
        pma.product_category,
        pma.month,
        round((pma.product_customers * 100.0 / mac.total_active_customers), 2) as adoption_rate
        
    from product_monthly_adoption pma
    join monthly_active_customers mac on pma.month = mac.month

),

adoption_trends as (
    
    select 
        product_id,
        product_name,
        product_category,
        month,
        adoption_rate,
        lag(adoption_rate, 1) over (partition by product_id order by month) as prev_month,
        lag(adoption_rate, 2) over (partition by product_id order by month) as prev_2_month,
        case 
        when lag(adoption_rate, 2) over (partition by product_id order by month) > 0 then 
            round((adoption_rate - lag(adoption_rate, 2) over (partition by product_id order by month)) / 
                lag(adoption_rate, 2) over (partition by product_id order by month) * 100, 2)
        else null 
        end as product_momentum

    from monthly_adoption

),

final as (

    select 
        month,
        round(avg(product_momentum), 2) as average_adoption_momentum,
        count(product_momentum) as products_with_momentum_data,
        round(stddev(product_momentum), 2) as momentum_volatility,
        -- top growing products
        string_agg(case when product_momentum >= 10 then concat(product_name, ' (+', cast(product_momentum as string), '%)') end, ', ' limit 3) as top_growing_products,
        -- declining products
        string_agg(case when product_momentum <= -10 then concat(product_name, ' (', cast(product_momentum as string), '%)') end, ', ' limit 3) as declining_products
        
    from adoption_trends
    where prev_2_month is not null
    group by month
    having count(product_momentum) >= 3
    order by month desc

)

select *
from final
