{% macro run_quality_checks_pre_hook(this_table) %}
  {# Hook à exécuter avant la création des modèles pour vérifier la qualité des sources #}
  
  {% if execute %}
    {% set schema_src = var('schema_src') %}
    
    {{ log('🔍 Démarrage des contrôles qualité des sources...', info=true) }}

    {% set table_str = this_table.render() %}

    {% set quality_sql = generate_quality_check(table_str, schema_src, is_source=true) %}
      
    {{ log('Contrôle qualité source pour: ' ~ table_str, info=true) }}
    {{ log('Query = \n' ~ quality_sql, info=true) }}
      
    {% set results = run_query(quality_sql) %}
      
    {{ log('✅ Contrôle qualité source terminé pour: ' ~ table_str, info=true) }}
    
    {{ log('✅ Tous les contrôles qualité des sources sont terminés', info=true) }}
  {% endif %}
{% endmacro %}

{% macro run_quality_checks_post_hook(this_table) %}
  {# Hook à exécuter après la création des modèles pour vérifier la qualité des destinations #}
  
  {% if execute %}
    {% set schema_dest = var('schema_dest') %}
    
    {{ log('🔍 Démarrage des contrôles qualité des destinations...', info=true) }}

    {% set table_str = this_table.render() %}

    {% set quality_sql = generate_quality_check(table_str, schema_dest, is_source=false) %}
      
    {{ log('Contrôle qualité destination pour: ' ~ table_str, info=true) }}
      
    {% set results = run_query(quality_sql) %}
      
    {{ log('✅ Contrôle qualité destination terminé pour: ' ~ table_str, info=true) }}
    
    {{ log('✅ Tous les contrôles qualité des destinations sont terminés', info=true) }}
  {% endif %}
{% endmacro %}
