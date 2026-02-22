{% macro privacy__mask_column(column) %}
    {# Extract metadata from the column definition #}
    {% set meta = column.meta or {} %}
    {% set method = meta.get('anonymization_method') %}
    {% set col_name = column.name %}

    {# Route to the appropriate anonymization function based on method #}
    {% if method == 'mask_partial' %}
        {# Phone number: partial masking #}
        {{ privacy__mask_phone(col_name) }}

    {% elif method == 'hash_sha256' %}
        {# Email anonymization: irreversible hash #}
        {{ privacy__mask_email(col_name) }}

    {% elif method == 'round_2_decimals' %}
        {# GPS coordinates: rounding to 2 decimals #}
        {{ privacy__mask_coordinates(col_name) }}

    {% elif method == 'suppress' %}
        {# Complete suppression: return NULL #}
        null as {{ col_name }}_anon

    {% else %}
        {# 
           No anonymization method defined OR column is not PII.
           Pass through with '_anon' suffix for consistency.
        #}
        {{ col_name }} as {{ col_name }}_anon
    {% endif %}
{% endmacro %}