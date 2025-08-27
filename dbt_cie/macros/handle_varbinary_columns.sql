{% macro handle_varbinary_columns(table_name, schema_src=var('schema_src')) %}
  {# Macro pour gérer automatiquement les colonnes varbinary sur Trino #}

  {% set query %}
    show columns from {{ schema_src }}.{{ table_name }}
  {% endset %}

  {% if execute %}
    {% set results = run_query(query) %}
    {% set varbinary_columns = [] %}
    {% set regular_columns = [] %}

    {% for row in results %}
      {% set column_name = row[0] %}
      {% set column_type = row[1] %}
      {% if 'varbinary' in column_type|lower %}
        {% do varbinary_columns.append(column_name) %}
      {% else %}
        {% do regular_columns.append(column_name) %}
      {% endif %}
    {% endfor %}

    {# Colonnes régulières d'abord #}
    {% set column_list = [] %}
    {% for col in regular_columns %}
      {% do column_list.append("t." ~ col) %}
    {% endfor %}

    {# Colonnes varbinary avec conversion #}
    {% for col in varbinary_columns %}
      {% do column_list.append("cast(to_hex(t." ~ col ~ ") as varchar) as " ~ col) %}
    {% endfor %}

    {{ return(column_list | join(',\n        ')) }}
  {% else %}
    {{ return("t.*") }}
  {% endif %}
{% endmacro %}
