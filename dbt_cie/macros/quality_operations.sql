{% macro log_quality_metrics(table_name, schema, metrics_dict, is_source=true) %}
  {# Macro pour enregistrer les métriques dans les tables de qualité #}
  
  {% set data_quality_table = var('schema_quality') ~ '.' ~ ('source_data_quality' if is_source else 'dest_data_quality') %}
  {% set data_table_ref = var('schema_quality') ~ '.data_table' %}
  {% set data_quality_ref = var('schema_quality') ~ '.data_quality' %}
  {% set tag = var('quality_tag') ~ '_' ~ modules.datetime.datetime.now().strftime("%d-%m-%Y") %}
  {% set table_full = schema ~ '.' ~ table_name %}

  {% for metric_name, metric_value in metrics_dict.items() %}
    {% set insert_sql %}
      INSERT INTO {{ data_quality_table }}(data_value, date_verification, table_id, data_quality_id, tag)
      VALUES (
        {{ metric_value }},
        CURRENT_DATE,
        (SELECT id FROM {{ data_table_ref }} WHERE label = '{{ table_full }}'),
        (SELECT id FROM {{ data_quality_ref }} WHERE label = '{{ metric_name }}'),
        '{{ tag }}'
      )
    {% endset %}
    
    {% if execute %}
      {% do run_query(insert_sql) %}
    {% endif %}
  {% endfor %}
{% endmacro %}

{% macro get_table_metrics(table_name, schema) %}
  {# Macro pour récupérer les métriques d'une table #}
  
  {% set table_full = schema ~ '.' ~ table_name %}
  
  {% set metrics_sql %}
    SELECT 
      COUNT(*) as row_count,
      COUNT(*) - COUNT(createdon) as null_count_createdon,
      COUNT(DISTINCT date_trunc('month', createdon)) as distinct_partition_id
    FROM {{ table_full }}
  {% endset %}
  
  {% if execute %}
    {% set results = run_query(metrics_sql) %}
    {% if results %}
      {% set row = results[0] %}
      {% set metrics = {
        'row_count': row[0],
        'null_count_createdon': row[1],
        'distinct_partition_id': row[2]
      } %}
      {{ return(metrics) }}
    {% endif %}
  {% endif %}
  
  {{ return({}) }}
{% endmacro %}

{% macro run_quality_report() %}
  {# Macro pour générer un rapport de qualité complet #}
  
  {% if execute %}
    {% set table_list = var('table_list') %}
    {% set schema_src = var('schema_src') %}
    {% set schema_dest = var('schema_dest') %}
    
    {{ log('📊 Génération du rapport de qualité...', info=true) }}
    
    {% for table_name in table_list %}
      {{ log('--- Table: ' ~ table_name ~ ' ---', info=true) }}
      
      {% set source_metrics = get_table_metrics(table_name, schema_src) %}
      {% set dest_metrics = get_table_metrics(table_name, schema_dest) %}
      
      {% if source_metrics and dest_metrics %}
        {{ log('Source - Lignes: ' ~ source_metrics.row_count ~ ', NULL createdon: ' ~ source_metrics.null_count_createdon ~ ', Partitions: ' ~ source_metrics.distinct_partition_id, info=true) }}
        {{ log('Dest   - Lignes: ' ~ dest_metrics.row_count ~ ', NULL createdon: ' ~ dest_metrics.null_count_createdon ~ ', Partitions: ' ~ dest_metrics.distinct_partition_id, info=true) }}
        
        {% if source_metrics.row_count == dest_metrics.row_count %}
          {{ log('✅ Nombre de lignes identique', info=true) }}
        {% else %}
          {{ log('❌ ALERTE: Nombre de lignes différent!', info=true) }}
        {% endif %}
      {% endif %}
    {% endfor %}
    
    {{ log('📊 Rapport de qualité terminé', info=true) }}
  {% endif %}
{% endmacro %}

{% macro cleanup_old_quality_records(days_to_keep=30) %}
  {# Macro pour nettoyer les anciens enregistrements de qualité #}
  
  {% set source_quality_table = var('schema_quality') ~ '.source_data_quality' %}
  {% set dest_quality_table = var('schema_quality') ~ '.dest_data_quality' %}
  
  {% set cleanup_sql_source %}
    DELETE FROM {{ source_quality_table }}
    WHERE date_verification < CURRENT_DATE - INTERVAL '{{ days_to_keep }}' DAY
  {% endset %}
  
  {% set cleanup_sql_dest %}
    DELETE FROM {{ dest_quality_table }}
    WHERE date_verification < CURRENT_DATE - INTERVAL '{{ days_to_keep }}' DAY
  {% endset %}
  
  {% if execute %}
    {{ log('🧹 Nettoyage des anciens enregistrements de qualité (>' ~ days_to_keep ~ ' jours)...', info=true) }}
    {% do run_query(cleanup_sql_source) %}
    {% do run_query(cleanup_sql_dest) %}
    {{ log('✅ Nettoyage terminé', info=true) }}
  {% endif %}
{% endmacro %}