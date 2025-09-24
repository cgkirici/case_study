-- revenue-weighted portfolio adoption score
with rev as (

    select *
    from {{ ref('revenue') }}

),

monthly_periods as (

  select distinct date_trunc(order_date, month) as month
  from rev

),
--assumption: active customers have made at least one purchase within preceding 30 day period
monthly_active_customers as (

    select 
        mp.month,
        count(distinct r.user_id) as total_active_customers

    from monthly_periods mp
    join rev r 
      on r.order_date between date_sub(last_day(mp.month), interval 29 day) and last_day(mp.month)
    group by mp.month

),

product_metrics as (

    select 
        r.product_id,
        r.product_name,
        date_trunc(r.order_date, month) as month,
        count(distinct r.user_id) as product_customers,
        sum(r.net_amount) as revenue

    from rev r
    group by r.product_id, r.product_name, date_trunc(r.order_date, month)

),

product_adoption_with_rates as (

    select 
        pm.product_id,
        pm.product_name,
        pm.month,
        pm.product_customers,
        pm.revenue,
        mct.total_active_customers,
        round((pm.product_customers * 100.0 / mct.total_active_customers), 2) as adoption_rate

    from product_metrics pm
    join monthly_active_customers mct on pm.month = mct.month

),

monthly_totals as (

    select 
        month, 
        sum(revenue) as total_revenue

    from product_adoption_with_rates
    group by month

),

final as (

    select 
        pm.month,
        round(sum(pm.adoption_rate * pm.revenue / mt.total_revenue), 2) as weighted_adoption_score,
        mt.total_revenue,
        count(distinct pm.product_id) as products_in_portfolio

    from product_adoption_with_rates pm
    join monthly_totals mt on pm.month = mt.month
    group by pm.month, mt.total_revenue
    order by pm.month desc
    
)

select *
from final
