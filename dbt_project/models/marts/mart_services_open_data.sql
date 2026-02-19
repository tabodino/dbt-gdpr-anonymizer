/*
    Mart Model - Open Data Services
    =================================
    
    Final dataset optimized for Open Data publication.
    
    100% GDPR COMPLIANT - PUBLIC RELEASE AUTHORIZED
    
    This model contains only anonymized and aggregated data.
    No personal data is present.
    
    Use cases:
    - Publication on data.gouv.fr
    - Public visualizations (Tableau, Metabase)
    - Open data APIs
    - Public downloads
    - Statistical analysis
    
    Author: JML
    Date: February 2026
*/

{{
  config(
    materialized='table',
    schema='marts',
    tags=['marts', 'open_data', 'public', 'api_ready']
  )
}}

with anonymized as (
    
    select * from {{ ref('int_services_anonymized') }}

),

enriched as (

    select
        -- ========================================
        -- IDENTIFIERS
        -- ========================================
        service_id,
        service_name,
        
        -- ========================================
        -- CLASSIFICATION
        -- ========================================
        parent_organization,
        organization_type,
        
        -- Normalization of organization types for visualization
        case 
            when organization_type = 'ministere' then 'Ministère'
            when organization_type = 'autorite-administrative-independante' then 'Autorité Indépendante'
            when organization_type = 'etablissement-public' then 'Établissement Public'
            when organization_type = 'service-central' then 'Service Central'
            else 'Autre'
        end as organization_type_label,
        
        -- ========================================
        -- ANONYMIZED CONTACT INFORMATION
        -- ========================================
        contact_email_anon as contact_email,
        contact_phone_anon as contact_phone,
        country_code,
        email_domain,
        website,
        
        -- ========================================
        -- AGGREGATED LOCATION
        -- ========================================
        city,
        commune,
        department_code,
        
        -- Department-to-region mapping (simplified)
        case 
            when department_code in ('75', '77', '78', '91', '92', '93', '94', '95') then 'Île-de-France'
            when department_code in ('59', '62') then 'Hauts-de-France'
            when department_code in ('69', '01', '42', '63') then 'Auvergne-Rhône-Alpes'
            when department_code in ('13', '83', '84', '04', '05', '06') then 'Provence-Alpes-Côte d\'Azur'
            when department_code in ('33', '24', '40', '47', '64') then 'Nouvelle-Aquitaine'
            when department_code in ('31', '09', '12', '32', '46', '65', '81', '82') then 'Occitanie'
            else 'Autre région'
        end as region,
        
        -- Anonymized GPS coordinates
        latitude_anon as latitude,
        longitude_anon as longitude,
        geohash,
        
        insee_code,
        postal_code,
        
        -- ========================================
        -- FLAGS AND INDICATORS
        -- ========================================
        has_email,
        has_phone,
        has_address,
        has_coordinates,
        
        -- Data completeness indicator
        (cast(has_email as int) + 
         cast(has_phone as int) + 
         cast(has_address as int) + 
         cast(has_coordinates as int)) as data_completeness_score,
        
        case 
            when (cast(has_email as int) + cast(has_phone as int) + 
                  cast(has_address as int) + cast(has_coordinates as int)) >= 3 
            then 'Complet'
            when (cast(has_email as int) + cast(has_phone as int) + 
                  cast(has_address as int) + cast(has_coordinates as int)) = 2 
            then 'Partiel'
            else 'Minimal'
        end as data_quality_level,
        
        -- ========================================
        -- METADATA
        -- ========================================
        last_updated,
        anonymized_at,
        current_timestamp as mart_created_at,
        
        anonymization_version,
        
        -- Metadata for open data catalogues
        'RGPD Anonymizer v' || anonymization_version as processing_pipeline,
        'Conforme RGPD - Art. 4.5 (Pseudonymisation)' as legal_status,
        'Licence Ouverte / Open Licence' as license
        
    from anonymized

),

final as (

    select * from enriched
    
    -- Quality filters for publication
    where service_name is not null
      and organization_type is not null
      and data_completeness_score >= 1  -- At least one available contact method

)

select * from final
