with orders as (

    select *
    from {{ ref('stg__orders') }}

),

details as (

    select *
    from ref
)