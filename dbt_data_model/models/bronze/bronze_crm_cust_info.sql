{{
    config(
        materialized='table',
        schema='bronze',
        meta={
            'layer': 'bronze',
            'source_system': 'crm',
        }
    )
}}

SELECT
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    {{ transform_date('cst_create_date') }} as cst_create_date,
    CURRENT_TIMESTAMP as dwh_create_date,
    '{{ run_started_at }}' as dbt_run_timestamp,
    '{{ invocation_id }}' as dbt_invocation_id

FROM {{ source('crm_raw', 'cust_info') }}

WHERE cst_id IS NOT NULL