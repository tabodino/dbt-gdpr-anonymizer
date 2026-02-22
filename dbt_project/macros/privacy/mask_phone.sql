{#
    Phone Number Anonymization Macro
    =================================
    
    Partially masks a phone number while keeping only the first characters
    (country code + beginning of the number).
    
    Example:
        Input:  +33 1 72 60 58 70
        Output: +33 1 XX XX XX XX
    
    Args:
        column_name (str): Name of the column containing phone numbers
        keep_chars (int): Number of characters to keep (default: 6)
    
    Returns:
        str: SQL expression producing the masked phone number
    
    Usage:
        select
            {{ privacy__mask_phone('contact_phone') }} as contact_phone_anon
        from source
    
    Author: JML
    Date: February 2026
#}


{% macro privacy__mask_phone(column_name, keep_chars=6) %}
    
    case 
        when {{ column_name }} is not null and {{ column_name }} != '' then
            concat(
                -- Keeps the first N characters (country code + beginning of the number)
                substr(trim({{ column_name }}), 1, {{ keep_chars }}),
                -- Masks the remaining part
                ' XX XX XX XX'
            )
        else 
            null
    end

{% endmacro %}


{#
    Country Code Extraction Macro
    ===============================
    
    Extracts only the country code from a phone number (e.g., +33, +1).
    Useful for aggregated geographic analysis without exposing PII.
    
    Args:
        column_name (str): Name of the phone number column
    
    Returns:
        str: SQL expression extracting the country code
    
    Usage:
        select
            {{ extract_country_code('contact_phone') }} as country_code
        from source
#}


{% macro extract_country_code(column_name) %}
    
    case 
        when {{ column_name }} like '+%' then
            -- Extracts up to the first space after the +
            substr(
                {{ column_name }},
                1,
                position(' ' in {{ column_name }})
            )
        else 
            null
    end as {{ column_name }}_anon

{% endmacro %}


{#
    Phone Number Normalization Macro
    =================================
    
    Normalizes a phone number (removes spaces, dashes, dots, etc.)
    before anonymization.
    
    Args:
        column_name (str): Name of the phone number column
    
    Returns:
        str: SQL expression for normalization
#}


{% macro normalize_phone(column_name) %}
    
    replace(
        replace(
            replace(
                replace(trim({{ column_name }}), ' ', ''),
                '-', ''
            ),
            '.', ''
        ),
        '(', ''
    )

{% endmacro %}


{#
    Phone Number Validation Macro
    ===============================
    
    Checks whether a string looks like a valid phone number.
    
    Args:
        column_name (str): Name of the column to validate
    
    Returns:
        bool: SQL expression returning true if the phone number is valid
#}


{% macro is_valid_phone(column_name) %}
    
    (
        -- Format international (+XX ...)
        {{ column_name }} like '+%'
        or
        -- Format national (0X ...)
        {{ column_name }} like '0%'
    )
    and length({{ normalize_phone(column_name) }}) >= 10

{% endmacro %}
