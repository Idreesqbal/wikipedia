{% macro generate_schema_name(custom_schema_name=None, node=None) -%}
    {%- set ns = namespace(schema_name="", matched=false) -%}
    {%- set tests_list = ['null', 'unique_', 'dbt_expectations'] -%}

    {%- for i in tests_list -%}
        {%- if i in node.fqn[2] -%}
            {%- set ns.schema_name = 'quality_metrics' -%}
            {%- set ns.matched = true -%}
        {%- endif -%}
    {%- endfor -%}

    {%- if not ns.matched -%}
        {%- if node.fqn[1] == 'stg' -%}
            {%- set ns.schema_name = var('stg') -%}
        {%- elif node.fqn[1] == 'int' -%}
            {%- set ns.schema_name = var('int') -%}
        {%- elif node.fqn[1] == 'pub' -%}
            {%- set ns.schema_name = var('pub') -%}
        {%- elif node.fqn[0] == 're_data' -%}
            {%- set ns.schema_name = 'quality_metrics' -%}
        {%- else -%}
            {%- set ns.schema_name = var('stg') -%}
        {%- endif -%}
    {%- endif -%}
    
    {{ ns.schema_name }}

{%- endmacro %}
