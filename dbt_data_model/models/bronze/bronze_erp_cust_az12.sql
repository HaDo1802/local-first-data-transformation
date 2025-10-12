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
    cid,
    {{ transform_date('bdate') }} as bdate,
    gen,
    CURRENT_TIMESTAMP as dwh_create_date,
    '{{ run_started_at }}' as dbt_run_timestamp,
    '{{ invocation_id }}' as dbt_invocation_id

FROM {{ source('erp_raw', 'cust_az12') }}

WHERE cid IS NOT NULL