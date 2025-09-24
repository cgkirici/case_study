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

product_adoption as (

    select
        product_id,
        product_name,
        date_trunc(order_date, month) as month,
        count(distinct user_id) as product_customers
    from rev
    group by product_id, product_name, date_trunc(order_date, month)

),

basic_adoption as (

    select
        pa.product_id,
        pa.product_name,
        pa.month,
        pa.product_customers,
        mt.total_active_customers,
        round(pa.product_customers / mt.total_active_customers, 2) as adoption_rate

    from product_adoption pa
    join monthly_totals mt on pa.month = mt.month
    order by pa.month, adoption_rate desc

),

-- adoption among new customers only (more sensitive to market trends)
new_customers as (

    select 
        user_id, 
        date_trunc(min(order_date), month) as first_month

    from rev
    group by user_id

),

new_customer_product_purchases as (

    select 
        o.product_id,
        o.product_name,
        date_trunc(o.order_date, month) as month,
        o.user_id

    from rev o
    join new_customers nc on o.user_id = nc.user_id 
        and date_trunc(o.order_date, month) = nc.first_month

),

monthly_new_customer_totals as (

    select 
        first_month as month,
        count(distinct user_id) as new_customers_total
    from new_customers
    group by first_month

),

new_customer_adoption as (

    select 
        ncp.product_id,
        ncp.product_name,
        ncp.month,
        count(distinct ncp.user_id) as new_customers_of_product,
        mnct.new_customers_total,
        round((count(distinct ncp.user_id) / mnct.new_customers_total), 2) as new_customer_adoption_rate

    from new_customer_product_purchases ncp
    join monthly_new_customer_totals mnct on ncp.month = mnct.month
    group by ncp.product_id, ncp.product_name, ncp.month, mnct.new_customers_total
    order by ncp.month desc, new_customer_adoption_rate desc

),

final as (

    select
        basic_adoption.product_id,
        basic_adoption.product_name,
        basic_adoption.month,
        basic_adoption.product_customers,
        basic_adoption.total_active_customers,
        basic_adoption.adoption_rate,

        new_customer_adoption.new_customers_of_product,
        new_customer_adoption.new_customers_total,
        new_customer_adoption.new_customer_adoption_rate

    from basic_adoption
    left join new_customer_adoption on basic_adoption.product_id = new_customer_adoption.product_id
        and basic_adoption.month = new_customer_adoption.month
)   

select *
from final
