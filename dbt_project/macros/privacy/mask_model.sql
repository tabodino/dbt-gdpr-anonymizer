{% macro privacy__mask_model(relation) %}

{%- set source_node = none -%}
{%- if execute -%}
    {# We look for the source model metadata in the dbt graph to access column-level PII tags #}
    {%- set source_node = graph.nodes.values() | selectattr("name", "equalto", relation.identifier) | first -%}
{%- endif -%}

{%- set columns = adapter.get_columns_in_relation(relation) -%}

select
    {% for col in columns -%}
        {# For each column, we check if it is tagged as PII in the source model metadata #}
        {%- set col_yaml = (source_node.columns.values() | selectattr("name", "equalto", col.name) | first) if source_node else none -%}
        {%- set meta = col_yaml.meta if col_yaml else {} -%}
        
        {# If the column is marked as PII, we apply the appropriate masking based on its metadata #}
        {{ privacy__mask_column(col.name, meta) }}
        {%- if not loop.last %}, {% endif %}
    {% endfor %}
from {{ relation }}

{% endmacro %}