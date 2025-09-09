{% macro remove_quotes_from_last_segment(table_str) %}
    {% set parts = table_str.split('.') %}
    {% set last_part = parts[-1].strip('"') %}
    {% set new_str = parts[:-1] | join('.') ~ '.' ~ last_part %}
    {{ return(new_str) }}
{% endmacro %}
{% macro generate_quality_check(table_name, schema, is_source=true) %}
  {# Macro pour générer les contrôles qualité similaires au DAG Airflow #}

  {% set table_name = 
    (var('schema_src') ~ '.' ~ remove_quotes_from_last_segment(table_name).split('.')[-1])
    if is_source
    else remove_quotes_from_last_segment(table_name)
%}

  
  {% set data_quality_table = var('schema_quality') ~ '.' ~ ('source_data_quality' if is_source else 'dest_data_quality') %}
  {% set data_table_ref = var('schema_quality') ~ '.data_table' %}
  {% set data_quality_ref = var('schema_quality') ~ '.data_quality' %}
  {% set tag = var('quality_tag') ~ '_' ~ modules.datetime.datetime.now().strftime("%d-%m-%Y") %}

  {% set metrics_sql = {
    'row_count': "SELECT 'row_count' AS metric, COUNT(*) AS value FROM " ~ table_name,
    'null_count_createdon': "SELECT 'null_count_createdon' AS metric, COUNT(*) AS value FROM " ~ table_name ~ " WHERE createdon IS NULL",
    'distinct_partition_id': "SELECT 'distinct_partition_id' AS metric, COUNT(DISTINCT date_trunc('month',createdon)) AS value FROM " ~ table_name
  } %}

  {% set union_parts = [] %}
  {% for metric_name, metric_sql in metrics_sql.items() %}
    {% do union_parts.append(metric_sql) %}
  {% endfor %}

  {% set final_sql %}
    INSERT INTO {{ data_quality_table }}(data_value, date_verification, table_id, data_quality_id, tag)
    SELECT
        value,
        CURRENT_DATE,
        (SELECT id FROM {{ data_table_ref }} WHERE label = '{{ table_name }}'),
        (SELECT id FROM {{ data_quality_ref }} WHERE label = metric),
        '{{ tag }}'
    FROM (
        {{ union_parts | join('\nUNION ALL\n') }}
    ) AS metrics
  {% endset %}

  {{ return(final_sql) }}
{% endmacro %}
