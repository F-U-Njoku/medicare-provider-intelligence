with carrier as (
    select * from {{ ref('stg_carrier_2008_2010_a') }}
    union all
    select * from {{ ref('stg_carrier_2008_2010_b') }}
)

select * from carrier