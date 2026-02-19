/*
    dbt Test - Absence of PII in Marts
    ==================================
    
    This test verifies that no personal data is present
    in the marts tables intended for public publication.
    
    The test fails if:
    - A non-anonymized email is detected (not ending with @anonymized.gouv.fr)
    - A non-masked phone number is detected (not containing XX)
    - GPS coordinates with more than 2 decimal places are found
    
    Author: JML
    Date: February 2026
*/

-- Test 1: Check emails in mart_services_open_data
with email_check as (
    select 
        'mart_services_open_data' as table_name,
        'contact_email' as column_name,
        contact_email as value,
        'Non-anonymized email detected' as issue_type
    from {{ ref('mart_services_open_data') }}
    where contact_email is not null
      -- Must end with @anonymized.gouv.fr
      and contact_email not like '%@anonymized.gouv.fr'
),

-- Test 2: Check phone numbers
phone_check as (
    select 
        'mart_services_open_data' as table_name,
        'contact_phone' as column_name,
        contact_phone as value,
        'Non-masked phone number detected' as issue_type
    from {{ ref('mart_services_open_data') }}
    where contact_phone is not null
      -- Must contain XX for masking
      and contact_phone not like '%XX%'
),

-- Test 3: Check GPS precision (max 2 decimals)
gps_check as (
    select 
        'mart_services_open_data' as table_name,
        'latitude/longitude' as column_name,
        concat(cast(latitude as varchar), ', ', cast(longitude as varchar)) as value,
        'GPS coordinates too precise' as issue_type
    from {{ ref('mart_services_open_data') }}
    where latitude is not null
      and longitude is not null
      -- Ensure coordinates have at most 2 decimal places
      and (
          length(split_part(cast(latitude as varchar), '.', 2)) > 2
          or length(split_part(cast(longitude as varchar), '.', 2)) > 2
      )
),

-- Combine all tests
all_issues as (
    select * from email_check
    union all
    select * from phone_check
    union all
    select * from gps_check
)

-- The test fails if any issues are found
select * from all_issues
