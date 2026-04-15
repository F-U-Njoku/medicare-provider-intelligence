with beneficiary as (
    select * from {{ ref('stg_beneficiary_2008') }}
    union all
    select * from {{ ref('stg_beneficiary_2009') }}
    union all
    select * from {{ ref('stg_beneficiary_2010') }}
)

select * from beneficiary
qualify row_number() over (partition by patient_id order by snapshot_year desc) = 1