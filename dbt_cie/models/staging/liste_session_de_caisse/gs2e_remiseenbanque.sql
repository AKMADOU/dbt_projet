-- {{ config(
--     materialized='table',
--     tags=['staging', 'cie'],
--     partition_by=['partition_id'],
--     file_format='parquet'
-- ) }}

-- with source_data as (
--     select * 
--     from {{ var('schema_src') }}.{{ this.name }} t
--     where t.createdon is not null
-- ),

-- transformed as (
--     select
--         date_trunc('month', t.createdon) as partition_id,
--         {{ handle_varbinary_columns(this.name) }}
--     from source_data as t
-- )

-- select * from transformed
{{ config(
    materialized='incremental',
    tags=['staging', 'cie'],
    partition_by=['partition_id'],
    file_format='parquet'
) }}

with source_data as (
    select * 
    from {{ var('schema_src') }}.{{ this.name }} t
    where t.createdon is not null
    {% if is_incremental() %}
      and t.createdon > (select max(partition_id) from {{ this }})
    {% endif %}
),

transformed as (
    select
        date_trunc('month', t.createdon) as partition_id,
        {{ handle_varbinary_columns(this.name) }}
    from source_data as t
)

select * from transformed