{#
    Email Anonymization Macro
    ==========================
    
    Transforms an email address into an irreversible hash while keeping
    a readable email format.
    
    Example:
        Input:  secretariat.sg@hatvp.fr
        Output: user_a3f7b2c1d4e5f6@anonymized.gouv.fr
    
    Args:
        column_name (str): Name of the column containing email addresses
        output_domain (str): Domain to use for anonymized emails
                             (default: anonymized.gouv.fr)
    
    Returns:
        str: SQL expression producing the anonymized email
    
    Usage:
        select
            {{ privacy__mask_email('contact_email') }} as contact_email_anon
        from source
    
    Author: JML
    Date: February 2026
#}


{% macro privacy__mask_email(column_name, output_domain='anonymized.gouv.fr') %}
    
    case 
        when {{ column_name }} is not null and {{ column_name }} != '' then
            concat(
                'user_',
                substr(
                    to_hex(
                        sha256(
                            concat(
                                lower(trim({{ column_name }})),
                                '{{ var("salt_key") }}'
                            )
                        )
                    ),
                    1,
                    16
                ),
                '@{{ output_domain }}'
            )
        else 
            null
    end as {{ column_name }}_anon

{% endmacro %}


{#
    Email Validation Macro
    =======================
    
    Checks whether a string is a valid email (basic format check).
    
    Args:
        column_name (str): Name of the column to validate
    
    Returns:
        bool: SQL expression returning true if the email is valid
#}


{% macro is_valid_email(column_name) %}
    
    {{ column_name }} like '%@%.%'
    and {{ column_name }} not like '%@%.@%'
    and length({{ column_name }}) >= 5

{% endmacro %}


{#
    Email Validation Macro
    =======================
    
    Checks whether a string is a valid email (basic format check).
    
    Args:
        column_name (str): Name of the column to validate
    
    Returns:
        bool: SQL expression returning true if the email is valid
#}


{% macro extract_email_domain(column_name) %}
    
    case 
        when {{ is_valid_email(column_name) }} then
            lower(
                substr(
                    {{ column_name }},
                    position('@' in {{ column_name }}) + 1
                )
            )
        else 
            null
    end

{% endmacro %}
