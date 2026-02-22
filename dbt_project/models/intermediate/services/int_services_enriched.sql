/*
    Intermediate Model - Enriched Services Dataset
    ============================================================
    
    This model enriches the anonymized dataset with:

    - organization category
    - department code
    - geohash
    - quality checks
    - business transformations

    IMPORTANT:

    - No anonymization logic here.
    - All privacy transformations happen upstream in:
      intermediate/privacy/int_services_anonymized.sql
*/

{{
  config(
    materialized='table',
    schema='intermediate',
    tags=['intermediate', 'services', 'enriched']
  )
}}

with source as (

    select * 
    from {{ ref('int_services_anonymized') }}

),

enriched as (

    select
        *,
        -- ========================================
        -- ORGANIZATION CATEGORY (business logic)
        -- ========================================
        case 
            when organization_type_anon in (
                'administration-centrale-ou-ministere',
                'cabinet-ministeriel',
                'service-a-competence-nationale',
                'secretaire-d-etat',
                'service-deconcentre'
            ) then 'services_centraux'

            when organization_type_anon in (
                'autorite-publique-independante',
                'autorite-administrative-independante'
            ) then 'autorites'

            when organization_type_anon in (
                'etablissement-public',
                'groupement-d-interet-public'
            ) then 'operateurs'

            when organization_type_anon = 'etablissement-d-enseignement'
            then 'enseignement'

            when organization_type_anon = 'ambassade-ou-mission-diplomatique'
            then 'diplomatie'

            when organization_type_anon in (
                'institution-europeenne',
                'institution'
            ) then 'institutions'

            when organization_type_anon = 'conseil-comite-commission-organisme-consultatif'
            then 'instances_consultatives'

            else 'autres'
        end as organization_category,

        -- ========================================
        -- DERIVED FIELDS
        -- ========================================
        substr(postal_code_anon, 1, 2) as department_code_anon,

        {{ create_geohash('latitude_anon', 'longitude_anon', precision=2) }} as geohash_anon,

        -- ========================================
        -- QUALITY CHECKS (post-anonymization)
        -- ========================================
        case 
            when contact_email_anon like '%@anonymized.gouv.fr'
            then 1 else 0
        end as is_email_properly_anonymized,

        case 
            when contact_phone_anon like '%XX XX XX XX'
            then 1 else 0
        end as is_phone_properly_anonymized

    from source
)

select * from enriched
