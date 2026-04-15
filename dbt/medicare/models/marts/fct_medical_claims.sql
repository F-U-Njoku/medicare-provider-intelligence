{{ config(
    partition_by={
      "field": "claims_start_date",
      "data_type": "date",
      "granularity": "month"
    }
) }}
with inpatient as (
    select *, 'Inpatient' as claim_category from {{ ref('stg_inpatient') }}
),
carrier as (
    select *, 'Carrier' as claim_category from {{ ref('stg_carrier') }}
),
unioned_claims as (
    select 
        patient_id, claim_id, segment, claims_start_date, claims_end_date, medicare_payment_amt, 
        total_patient_cost, primary_diagnosis_code, claim_category 
    from inpatient
    union all
    select 
        patient_id, claim_id, segment, claims_start_date, claims_end_date, medicare_payment_amt, 
        total_patient_cost, primary_diagnosis_code, claim_category 
    from carrier
)

select
    {{ dbt_utils.generate_surrogate_key(['c.claim_id', 'c.claim_category', 'c.segment']) }} as medical_claim_pk,
    c.*,
    date_diff(c.claims_start_date, p.birth_date, YEAR) as patient_age_at_claim,
    p.birth_date,
    p.gender,
    p.state_code
from unioned_claims c
left join {{ ref('stg_beneficiaries') }} p on c.patient_id = p.patient_id