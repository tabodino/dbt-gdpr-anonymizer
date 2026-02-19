/*
    Intermediate Model - Anonymized Services
    ========================================
    
    This model applies GDPR anonymization techniques to the sensitive
    data coming from the staging layer to produce a compliant dataset.
    
    ANONYMIZED DATA — PII SAFE
    
    Applied techniques:
    - Email: SHA-256 hash + salt → user_xxxxx@anonymized.gouv.fr
    - Phone: Partial masking → +33 1 XX XX XX XX
    - Address: Aggregated at the city level
    - GPS: Rounded to 2 decimals (~1km precision)
    
    This model can be used for:
    - Open Data publication
    - Public visualizations
    - Statistical analysis
    - Sharing with third parties
    
    GDPR compliance:
    - Art. 4.5: Pseudonymization applied
    - Art. 5.1.c: Data minimization
    - Art. 32: Processing security
    
    Author: JML
    Date: February 2026
*/


{{
  config(
    materialized='table',
    schema='anonymized',
    tags=['intermediate', 'anonymized', 'pii_safe', 'open_data_ready']
  )
}}

with source as (
    
    select * from {{ ref('stg_services_publics') }}

),

anonymized as (

    select
        -- ========================================
        -- IDENTIFIERS (kept)
        -- ========================================
        service_key,
        service_id,
        
        -- ========================================
        -- GENERAL INFORMATION (kept)
        -- ========================================
        service_name,
        parent_organization,
        organization_type,
        website,
        
        -- ========================================
        -- ANONYMIZED DATA
        -- ========================================
        
        -- Email : irreversible SHA-256 hash
        {{ anonymize_email('contact_email') }} as contact_email_anon,
        
        -- Phone : partial masking (keeps country code)
        {{ anonymize_phone('contact_phone', keep_chars=6) }} as contact_phone_anon,
        
        -- Country code for aggregated analysis
        {{ extract_country_code('contact_phone') }} as country_code,
        
        -- Email domain for aggregated analysis (no PII)
        {{ extract_email_domain('contact_email') }} as email_domain,
        
        -- ========================================
        -- AGGREGATED LOCATION
        -- ========================================
        
        -- Address: removed (too precise)
        null as street_address_removed,
        
        -- City: kept (acceptable aggregation)
        city,
        commune,
        
        -- Department: derived from postal code
        substr(postal_code, 1, 2) as department_code,
        
        postal_code,
        
        -- GPS coordinates: rounded to 2 decimals (~1km)
        {{ anonymize_coordinates('latitude', 'longitude', precision=2) }},
        
        -- Geohash for spatial aggregation
        {{ create_geohash('latitude', 'longitude', precision=2) }} as geohash,
        
        insee_code,
        
        -- ========================================
        -- TIMESTAMPS & METADATA
        -- ========================================
        last_updated,
        loaded_at,
        current_timestamp as anonymized_at,
        
        -- ========================================
        -- QUALITY FLAGS (kept)
        -- ========================================
        has_email,
        has_phone,
        has_address,
        has_coordinates,
        
        -- ========================================
        -- ANONYMIZATION METADATA
        -- ========================================
        '{{ var("project_version") }}' as anonymization_version,
        'SHA256+salt' as email_anonymization_method,
        'partial_masking' as phone_anonymization_method,
        'round_2_decimals' as gps_anonymization_method
        
    from source

),

quality_checks as (

    select
        *,
        
        -- Post‑anonymization quality checks
        case 
            when contact_email_anon is not null 
             and contact_email_anon like '%@anonymized.gouv.fr'
            then 1 else 0 
        end as is_email_properly_anonymized,
        
        case 
            when contact_phone_anon is not null 
             and contact_phone_anon like '%XX XX XX XX'
            then 1 else 0 
        end as is_phone_properly_anonymized,
        
        case 
            when latitude_anon is not null
             and longitude_anon is not null
             and latitude_anon between -90 and 90
             and longitude_anon between -180 and 180
            then 1 else 0 
        end as are_coordinates_valid
        
    from anonymized

),

final as (

    select * from quality_checks
    
    -- CRITICAL: Keep only properly anonymized records
    where 
        (has_email = 0 or is_email_properly_anonymized = 1)
        and (has_phone = 0 or is_phone_properly_anonymized = 1)
        and (has_coordinates = 0 or are_coordinates_valid = 1)

)

select * from final
