{{
    config(
        materialized='table',
        schema='bronze',
        meta={
            'layer': 'bronze',
            'source_system': 'crm',
            'refresh_frequency': 'daily'
        }
    )
}}

SELECT
    prd_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    {{ transform_date('prd_start_dt') }} as prd_start_dt,
    {{ transform_date('prd_end_dt') }} as prd_end_dt,
    CURRENT_TIMESTAMP as dwh_create_date,
    '{{ run_started_at }}' as dbt_run_timestamp,
    '{{ invocation_id }}' as dbt_invocation_id

FROM {{ source('crm_raw', 'prd_info') }}

-- Data quality filters
WHERE prd_id IS NOT NULL