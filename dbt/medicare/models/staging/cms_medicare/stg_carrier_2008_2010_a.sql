with source as (
    select *, 'A' as snapshot_batch
    from {{ source('cms_medicare', 'ext_carrier_2008_2010_part_a') }} 
),
carrier_2008_2010_a as (
    select
        -- 1. Identifiers
        cast(DESYNPUF_ID as STRING) as patient_id,
        cast(CLM_ID as STRING) as claim_id,
        '1' as segment,

        -- 2. Dates
        parse_date('%Y%m%d', cast(CLM_FROM_DT as STRING)) as claims_start_date,
        parse_date('%Y%m%d', cast(CLM_THRU_DT as STRING)) as claims_end_date,

        -- 3. Primary Clinical Codes 
        ICD9_DGNS_CD_1 as primary_diagnosis_code,
        HCPCS_CD_1 as primary_procedure_code,
        PRF_PHYSN_NPI_1 as provider_physician_npi,

        -- 4. Financials (The "Fuller View")
        -- What Medicare paid
        coalesce(cast(LINE_NCH_PMT_AMT_1 as FLOAT64), 0) as medicare_payment_amt,
        
        -- What the patient owes (Deductible + Coinsurance)
        coalesce(cast(LINE_BENE_PTB_DDCTBL_AMT_1 as FLOAT64), 0) as patient_deductible_amt,
        coalesce(cast(LINE_COINSRNC_AMT_1 as FLOAT64), 0) as patient_coinsurance_amt,
        
        -- What a third party paid
        coalesce(cast(LINE_BENE_PRMRY_PYR_PD_AMT_1 as FLOAT64), 0) as primary_payer_paid_amt,
        
        -- Total Negotiated Rate (Allowed Charge)
        coalesce(cast(LINE_ALOWD_CHRG_AMT_1 as FLOAT64), 0) as allowed_charge_amt,

        -- 5. Derived Logic: Total Patient Responsibility
        (coalesce(cast(LINE_BENE_PTB_DDCTBL_AMT_1 as FLOAT64), 0) + 
        coalesce(cast(LINE_COINSRNC_AMT_1 as FLOAT64), 0)) as total_patient_cost,

        -- 6. Metadata
        snapshot_batch
    from source
    where DESYNPUF_ID is not null 
      and CLM_ID is not null
)

select * from carrier_2008_2010_a