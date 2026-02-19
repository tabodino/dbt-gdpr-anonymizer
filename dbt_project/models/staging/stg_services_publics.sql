/*
    Staging Model - Public Services
    ===============================
    
    This model cleans and types the raw data coming from the seed.
    
    WARNING: This model contains Personal Identifiable Information (PII)
    
    Sensitive data present:
    - contact_email : Personal email address
    - contact_phone : Direct phone number
    - street_address : Full postal address
    - latitude/longitude : Exact GPS coordinates
    
    Do NOT use this model for public exports or visualizations.
    Use int_services_anonymized instead.
    
    Author: JML
    Date: February 2026
*/


{{
  config(
    materialized='view',
    schema='staging',
    tags=['staging', 'pii_present']
  )
}}

with source as (
    
    select * from {{ ref('services_publics_raw') }}

),

cleaned as (

    select
        -- Identifiers
        service_id,
        
        -- Basic information
        service_name,
        parent_organization,
        organization_type,
        
        -- SENSITIVE DATA (PII)
        contact_email,
        contact_phone,
        website,
        
        -- Address (indirect PII)
        street_address,
        postal_code,
        city,
        commune,
        
        -- GPS coordinates (indirect PII)
        cast(latitude as double) as latitude,
        cast(longitude as double) as longitude,
        
        -- Geographical metadata (non‑PII)
        insee_code,
        
        -- Timestamps
        cast(last_updated as date) as last_updated,
        current_timestamp as loaded_at
        
    from source
    
    where service_id is not null
      and service_name is not null

),

final as (

    select
        -- Generate a unique surrogate key
        {{ dbt_utils.generate_surrogate_key(['service_id']) }} as service_key,
        
        *,
        
        -- Data quality flags
        case when contact_email is not null then 1 else 0 end as has_email,
        case when contact_phone is not null then 1 else 0 end as has_phone,
        case when street_address is not null then 1 else 0 end as has_address,
        case when latitude is not null and longitude is not null then 1 else 0 end as has_coordinates
        
    from cleaned

)

select * from final
