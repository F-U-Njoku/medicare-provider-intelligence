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
        total_patient_cost, primary_diagnosis_code, claim_category, length_of_stay_days
    from inpatient
    union all
    select 
        patient_id, claim_id, segment, claims_start_date, claims_end_date, medicare_payment_amt, 
        total_patient_cost, primary_diagnosis_code, claim_category, 0 as length_of_stay_days
    from carrier
)

select
    {{ dbt_utils.generate_surrogate_key(['c.claim_id', 'c.claim_category', 'c.segment']) }} as medical_claim_pk,
    c.*,
    date_diff(c.claims_start_date, p.birth_date, YEAR) as patient_age_at_claim,
    (c.medicare_payment_amt + c.total_patient_cost) as total_gross_claim_amt,
    d.description as primary_diagnosis_description,
    p.birth_date,
    p.gender,
    p.state_code
from unioned_claims c
left join {{ ref('stg_beneficiaries') }} p on c.patient_id = p.patient_id
left join {{ref("icd9_codes") }} d on c.primary_diagnosis_code = d.icd9_code
where c.claims_start_date >= '2008-01-01' 
  and c.claims_start_date <= '2010-12-31'