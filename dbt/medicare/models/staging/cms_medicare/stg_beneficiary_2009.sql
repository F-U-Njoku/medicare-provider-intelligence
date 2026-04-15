with source as (
    select *, '2009' as year
    from {{ source('cms_medicare', 'ext_beneficiary_2009') }} 
),
beneficiary_2009 as (
    select
        -- 1. Identifiers
        cast(DESYNPUF_ID as string) as patient_id,

        -- 2. Demographics 
        parse_date('%Y%m%d', cast(BENE_BIRTH_DT as STRING)) as birth_date,
        parse_date('%Y%m%d', cast(BENE_DEATH_DT as STRING)) as death_date,
        case when BENE_SEX_IDENT_CD = 1 then 'Male' 
            when BENE_SEX_IDENT_CD = 2 then 'Female'
            else 'Unknown'
        end as gender,
        case when BENE_DEATH_DT is not null then 'Deceased' 
            else 'Alive' 
        end as vital_status,

        -- 3. Geography
        cast(SP_STATE_CODE as string) as state_code,

        -- 4. Metadata
        year as snapshot_year
    from source
    where DESYNPUF_ID is not null
)

select * from beneficiary_2009