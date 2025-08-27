-- Test personnalisé pour vérifier la cohérence des comptages de lignes
-- Ce test compare le nombre de lignes entre source et destination

{% set tables_to_check = var('table_list') %}

{% for table in tables_to_check %}
with source_{{ table }}_count as (
    select 
        '{{ table }}' as table_name,
        count(*) as source_count
    from {{ source('cie_source', table) }}
    where createdon is not null
),

staging_{{ table }}_count as (
    select 
        '{{ table }}' as table_name,
        count(*) as staging_count  
    from {{ ref('stg_' + table) }}
),

marts_{{ table }}_count as (
    select 
        '{{ table }}' as table_name,
        count(*) as marts_count
    from {{ ref('dim_' + table) }}
)

{% if not loop.last %},{% endif %}
{% endfor %}

-- Union de tous les résultats
{% for table in tables_to_check %}
select 
    s.table_name,
    s.source_count,
    st.staging_count,
    m.marts_count,
    case 
        when s.source_count = st.staging_count 
         and st.staging_count = m.marts_count then 'OK'
        else 'MISMATCH'
    end as status
from source_{{ table }}_count s
join staging_{{ table }}_count st on s.table_name = st.table_name  
join marts_{{ table }}_count m on s.table_name = m.table_name
where s.source_count != st.staging_count 
   or st.staging_count != m.marts_count

{% if not loop.last %}
union all
{% endif %}
{% endfor %}