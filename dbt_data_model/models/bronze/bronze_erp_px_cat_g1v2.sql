{{
    config(
        materialized='table',
        schema='bronze',
        meta={
            'layer': 'bronze',
            'source_system': 'erp',
            'refresh_frequency': 'daily'
        }
    )
}}


SELECT
    id,
    cat,
    subcat,
    maintenance,
    CURRENT_TIMESTAMP as dwh_create_date,
    '{{ run_started_at }}' as dbt_run_timestamp,
    '{{ invocation_id }}' as dbt_invocation_id

FROM {{ source('erp_raw', 'px_cat_g1v2') }}


WHERE id IS NOT NULL