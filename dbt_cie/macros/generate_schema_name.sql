{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {%- if target.name == 'prod' -%}
            {{ custom_schema_name | trim }}
        {%- else -%}
            {{ default_schema }}_{{ custom_schema_name | trim }}
        {%- endif -%}

    {%- endif -%}

{%- endmacro %}


{% macro generate_quality_tag() %}
  {#- Génère un tag unique pour le contrôle qualité -#}
  {% set current_date = modules.datetime.datetime.now().strftime("%d-%m-%Y") %}
  {{ return(var('quality_tag') ~ '_' ~ current_date) }}
{% endmacro %}