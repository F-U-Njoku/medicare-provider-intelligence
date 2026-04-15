with all_providers as (
    select attending_physician_npi as npi from {{ ref('stg_inpatient') }}
    union distinct
    select operating_physician_npi as npi from {{ ref('stg_inpatient') }}
    union distinct
    select provider_physician_npi as npi from {{ ref('stg_carrier') }}
)

select
    -- Generate a unique surrogate key for each provider based on their NPI
    {{ dbt_utils.generate_surrogate_key(['npi']) }} as provider_key,
    npi as provider_npi
from all_providers
where npi is not null