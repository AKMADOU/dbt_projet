-- Test pour vérifier les valeurs nulles dans createdon
-- Ce test identifie les enregistrements avec createdon NULL dans les sources

{% set tables_to_check = var('table_list') %}

{% for table in tables_to_check %}
select 
    '{{ table }}' as table_name,
    'source' as layer,
    count(*) as null_createdon_count
from {{ source('cie_source', table) }}
where createdon is null

{% if not loop.last %}
union all
{% endif %}
{% endfor %}

-- Ne retourner des résultats que s'il y a des valeurs nulles
having count(*) > 0