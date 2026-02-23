{#
    PII Report Generation Macro
    ============================
    
    Iterates through all dbt models and extracts columns marked
    with meta.pii = true to generate an inventory report.
    
    This report is essential for:
    - GDPR audits
    - DPO documentation
    - CNIL compliance
    
    Usage:
        dbt run-operation generate_pii_report
    
    Author: JML
    Date: February 2026
#}

{% macro generate_pii_report() %}

    {% set pii_columns = [] %}
    
    {# Iterate through the dbt graph #}
    {% for node in graph.nodes.values() %}
        
        {# Process only models (exclude seeds, tests, etc.) #}
        {% if node.resource_type == 'model' %}
            
            {# Iterate through the model's columns #}
            {% for col_name, col_def in node.columns.items() %}
                
                {# Check if the column is marked as PII #}
                {% if col_def.meta.pii %}
                    
                    {% set pii_info = {
                        'model_name': node.name,
                        'model_schema': node.schema,
                        'column_name': col_name,
                        'column_description': col_def.description,
                        'pii_type': col_def.meta.pii_type | default('unspecified'),
                        'anonymization_method': col_def.meta.anonymization_method | default('none'),
                        'data_owner': col_def.meta.data_owner | default('undefined'),
                        'legal_basis': col_def.meta.legal_basis | default('not_specified'),
                        'retention_days': col_def.meta.retention_days | default(var('retention_days_default')),
                        'k_anonymity_target': col_def.meta.k_anonymity_target | default('N/A')
                    } %}
                    
                    {% do pii_columns.append(pii_info) %}
                    
                {% endif %}
            {% endfor %}
        {% endif %}
    {% endfor %}
    
    {# Generate SQL to create the PII report table #}
    {% if pii_columns | length > 0 %}
        
        {{ log("Generating PII report: " ~ (pii_columns | length) ~ " columns found", info=true) }}
        
        with pii_inventory as (
            
            {% for pii_info in pii_columns %}
            select
                '{{ pii_info.model_name }}' as model_name,
                '{{ pii_info.model_schema }}' as model_schema,
                '{{ pii_info.column_name }}' as column_name,
                '{{ pii_info.column_description }}' as column_description,
                '{{ pii_info.pii_type }}' as pii_type,
                '{{ pii_info.anonymization_method }}' as anonymization_method,
                '{{ pii_info.data_owner }}' as data_owner,
                '{{ pii_info.legal_basis }}' as legal_basis,
                {{ pii_info.retention_days }} as retention_days,
                '{{ pii_info.k_anonymity_target }}' as k_anonymity_target,
                current_timestamp as report_generated_at
            {{ "union all" if not loop.last else "" }}
            {% endfor %}
        )
        
        select * from pii_inventory
        order by model_name, column_name
        
    {% else %}
        
        {{ log("No PII columns found in the models", info=true) }}
        
        select
            'No PII columns found' as message,
            current_timestamp as report_generated_at
        
    {% endif %}

{% endmacro %}


{#
    PII Summary Logging Macro
    ==========================
    
    Displays a summary in the logs at the end of execution.
#}

{% macro log_pii_summary() %}

    {% set pii_count = 0 %}
    {% set models_with_pii = [] %}
    
    {% for node in graph.nodes.values() %}
        {% if node.resource_type == 'model' %}
            {% set has_pii = false %}
            {% for col_name, col_def in node.columns.items() %}
                {% if col_def.meta.pii %}
                    {% set pii_count = pii_count + 1 %}
                    {% set has_pii = true %}
                {% endif %}
            {% endfor %}
            {% if has_pii %}
                {% do models_with_pii.append(node.name) %}
            {% endif %}
        {% endif %}
    {% endfor %}
    
    {{ log("", info=true) }}
    {{ log("=" * 60, info=true) }}
    {{ log("PII SUMMARY", info=true) }}
    {{ log("=" * 60, info=true) }}
    {{ log("PII columns detected: " ~ pii_count, info=true) }}
    {{ log("Models containing PII: " ~ (models_with_pii | length), info=true) }}
    
    {% if models_with_pii | length > 0 %}
        {{ log("   → " ~ (models_with_pii | join(", ")), info=true) }}
    {% endif %}
    
    {{ log("=" * 60, info=true) }}
    {{ log("", info=true) }}

{% endmacro %}


{#
    PII Metadata Validation Macro
    ==============================
    
    Checks that all PII columns have a defined anonymization method.
#}

{% macro validate_pii_metadata() %}

    {% set invalid_pii = [] %}
    
    {% for node in graph.nodes.values() %}
        {% if node.resource_type == 'model' %}
            {% for col_name, col_def in node.columns.items() %}
                {% if col_def.meta.pii %}
                    {% if not col_def.meta.anonymization_method %}
                        {% do invalid_pii.append({
                            'model': node.name,
                            'column': col_name
                        }) %}
                    {% endif %}
                {% endif %}
            {% endfor %}
        {% endif %}
    {% endfor %}
    
    {% if invalid_pii | length > 0 %}
        {{ log("WARNING: PII columns without anonymization method:", info=true) }}
        {% for item in invalid_pii %}
            {{ log("   - " ~ item.model ~ "." ~ item.column, info=true) }}
        {% endfor %}
        {{ exceptions.raise_compiler_error("Unsecured PII columns detected") }}
    {% else %}
        {{ log("All PII columns have a defined anonymization method", info=true) }}
    {% endif %}

{% endmacro %}
