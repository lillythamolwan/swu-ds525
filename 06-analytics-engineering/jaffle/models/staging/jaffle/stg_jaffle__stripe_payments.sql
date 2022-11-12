with

source as (

    select * from {{ source('jaffle', 'stripe_payments') }}

)

, final as (

    select
        id
        , order_id
        , payment_method
        , status
        , amount
        , created

    from source

)

select * from final