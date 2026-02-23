/*
    Intermediate Model — Automatic GDPR Anonymization
    =================================================

    This model does NOT contain manual anonymization logic.

    Instead, it uses the `privacy__mask_model()` macro, which:
    - reads the metadata defined in schema.yml (pii, anonymization_method, etc.)
    - automatically applies the correct anonymization rule per column
    - produces a PII‑safe dataset without writing CASE WHEN logic

    Examples of supported anonymization methods (driven by meta):
    - hash_sha256          → irreversible hashing for direct identifiers
    - mask_partial         → partial masking for phone numbers
    - round_2_decimals     → GPS rounding (~1 km precision)
    - aggregate_to_city    → address aggregation (if configured)

    Purpose:
    - Provide a clean, compliant, automatically anonymized dataset
    - Serve as the input for downstream business transformations
    - Guarantee consistency and eliminate human error

    Important:
    - All business logic (categorization, geohash, quality checks, etc.)
      is handled in `int_services_enriched.sql`, not here.
    
    Author: JML
    Date: February 2026
*/


{{
  config(
    materialized='table',
    schema='anonymized',
    tags=['intermediate', 'privacy', 'pii_safe']
  )
}}


with masked_data as (
    {{ privacy__mask_model(ref('stg_services_publics')) }}
)

select 
    *,
    current_timestamp as anonymized_at,
    '{{ var("project_version") }}' as anonymization_version,
    'round_{{ var("gps_precision") }}_decimals' as gps_anonymization_method
from masked_data