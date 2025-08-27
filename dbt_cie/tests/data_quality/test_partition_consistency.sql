-- Test pour vérifier la cohérence des partitions
-- Ce test s'assure que toutes les partitions ont des données cohérentes

{% set tables_to_check = var('table_list') %}

with partition_stats as (
{% for table in tables_to_check %}
    select 
        '{{ table }}' as table_name,
        partition_id,
        count(*) as record_count,
        min(createdon) as min_createdon,
        max(createdon) as max_createdon
    from {{ ref('dim_' + table) }}
    group by partition_id
    
    {% if not loop.last %}
    union all
    {% endif %}
{% endfor %}
),

partition_issues as (
    select 
        table_name,
        partition_id,
        record_count,
        min_createdon,
        max_createdon,
        case 
            when date_trunc('month', min_createdon) != partition_id then 'MIN_DATE_MISMATCH'
            when date_trunc('month', max_createdon) != partition_id then 'MAX_DATE_MISMATCH'
            when record_count = 0 then 'EMPTY_PARTITION'
            else 'OK'
        end as issue_type
    from partition_stats
)

select *
from partition_issues  
where issue_type != 'OK'