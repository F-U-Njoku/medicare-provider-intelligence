with source as (
    select *
    from {{ source('cms_medicare', 'ext_inpatient_2008_2010') }} 
),

inpatient_claims as (
    select
        -- 1. Identifiers
        cast(DESYNPUF_ID as STRING) as patient_id,
        cast(CLM_ID as STRING) as claim_id,
        cast(PRVDR_NUM as STRING) as provider_institution_id,
        cast(SEGMENT as STRING) as segment,

        -- 2. Dates
        parse_date('%Y%m%d', cast(CLM_FROM_DT as STRING)) as claims_start_date,
        parse_date('%Y%m%d', cast(CLM_THRU_DT as STRING)) as claims_end_date,
        parse_date('%Y%m%d', cast(CLM_ADMSN_DT as STRING)) as admission_date,
        parse_date('%Y%m%d', cast(NCH_BENE_DSCHRG_DT as STRING)) as discharge_date,

        -- 3. Clinical Codes 
        cast(CLM_DRG_CD as STRING) as drg_code, -- Diagnosis Related Group (Crucial for Inpatient)
        ICD9_DGNS_CD_1 as primary_diagnosis_code,
        ICD9_PRCDR_CD_1 as primary_procedure_code,
        AT_PHYSN_NPI as attending_physician_npi,
        OP_PHYSN_NPI as operating_physician_npi,

        -- 4. Financials
        -- What Medicare paid (Header level)
        coalesce(cast(CLM_PMT_AMT as FLOAT64), 0) as medicare_payment_amt,
        
        -- Primary Payer (Third party)
        coalesce(cast(NCH_PRMRY_PYR_CLM_PD_AMT as FLOAT64), 0) as primary_payer_paid_amt,

        -- Patient Responsibility (Deductible + Coinsurance + Blood Deductible)
        coalesce(cast(NCH_BENE_IP_DDCTBL_AMT as FLOAT64), 0) as inpatient_deductible_amt,
        coalesce(cast(NCH_BENE_PTA_COINSRNC_LBLTY_AM as FLOAT64), 0) as patient_coinsurance_amt,
        coalesce(cast(NCH_BENE_BLOOD_DDCTBL_LBLTY_AM as FLOAT64), 0) as blood_deductible_amt,

        -- 5. Derived Logic
        -- Total Patient Responsibility
        (coalesce(cast(NCH_BENE_IP_DDCTBL_AMT as FLOAT64), 0) + 
         coalesce(cast(NCH_BENE_PTA_COINSRNC_LBLTY_AM as FLOAT64), 0) +
         coalesce(cast(NCH_BENE_BLOOD_DDCTBL_LBLTY_AM as FLOAT64), 0)) as total_patient_cost,
        
        -- Length of Stay (Utilization days)
        cast(CLM_UTLZTN_DAY_CNT as INT64) as length_of_stay_days
        
    from source
    -- Basic data quality filter
    where DESYNPUF_ID is not null 
      and CLM_ID is not null
)

select * from inpatient_claims