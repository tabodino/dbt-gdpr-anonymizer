{% macro privacy__mask_column(column_name, meta=none) %}
    {# Extract metadata from the column definition #}
    {% set meta = meta or {} %}
    {% set method = meta.get('anonymization_method') %}

    {# Route to the appropriate anonymization function based on method #}
    {% if method == 'mask_partial' %}
        {# Phone number: partial masking #}
        {{ privacy__mask_phone(column_name) }}

    {% elif method == 'hash_sha256' %}
        {# Email anonymization: irreversible hash #}
        {{ privacy__mask_email(column_name) }}

    {% elif method == 'round_2_decimals' %}
        {# GPS coordinates: rounding to 2 decimals #}
        {{ privacy__mask_coordinates(column_name) }}

    {% elif method == 'suppress' %}
        {# Complete suppression: return NULL #}
        null as {{ column_name }}_anon

    {% else %}
        {# 
           No anonymization method defined OR column is not PII.
           Pass through with '_anon' suffix for consistency.
        #}
        {{ column_name }} as {{ column_name }}_anon
    {% endif %}
{% endmacro %}