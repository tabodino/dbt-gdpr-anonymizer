{#
    Meta-Driven Anonymization Macro
    ===============================
    
    This macro automates the generation of an anonymized SQL model by:
    1. Inspecting physical columns in the source relation.
    2. Applying masking logic based on 'anonymization_method' in YAML meta.
    3. Injecting "virtual" technical columns defined as 'generated' in YAML.

    Args:
        relation (dbt relation): The source model/staging to anonymize.
#}

{% macro privacy__mask_model(relation) %}

{%- set model_name = relation.identifier -%}
{%- set model_node = none -%}

{# 
   The 'graph' object is only fully available during the 'execute' phase.
   Checking 'if execute' prevents compilation errors during the parsing phase.
#}
{%- if execute -%}
    {%- set model_node = graph.nodes.values() | selectattr("name", "equalto", model_name) | first -%}
{%- endif -%}

{# Fetch physical columns from the database catalog using the adapter #}
{%- set physical_columns = adapter.get_columns_in_relation(relation) -%}

select
    {# 
       PHASE 1: Physical Data Masking
       Loop through actual columns found in the source table.
    #}
    {% for col in physical_columns -%}
        {{ privacy__mask_column(col) }}{%- if not loop.last or (model_node and model_node.columns | length > 0) %}, {% endif %}
    {% endfor -%}

    {# 
       PHASE 2: Technical Metadata Injection
       Inject columns defined in YAML with 'meta: {generated: true}'.
       This ensures compliance fields (versions, timestamps) are always present.
    #}
    {%- if model_node and model_node.columns -%}
        {%- for col_name, col_config in model_node.columns.items() -%}
            {%- if col_config.meta.generated -%}
                {{ col_config.meta.expression }} as {{ col_name }}
                {# Add a comma if this is not the last generated column #}
                {%- if not loop.last %}, {% endif %}
            {%- endif -%}
        {%- endfor -%}
    {%- endif %}

from {{ relation }}

{% endmacro %}