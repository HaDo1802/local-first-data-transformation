{{
    config(
        materialized='incremental',
        unique_key='sls_ord_num',
        schema='bronze',
        on_schema_change='append_new_columns',
        meta={
            'layer': 'bronze',
            'source_system': 'crm',
            'refresh_frequency': 'daily'
        }
    )
}}

SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    {{transform_date('sls_order_dt')}} as sls_order_dt,
    {{transform_date('sls_ship_dt')}} as sls_ship_dt,
    {{transform_date('sls_due_dt')}} as sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price,
    CURRENT_TIMESTAMP as dwh_create_date,
    '{{ run_started_at }}' as dbt_run_timestamp,
    '{{ invocation_id }}' as dbt_invocation_id

FROM {{ source('crm_raw', 'sales_details') }}

WHERE sls_ord_num IS NOT NULL
  AND sls_prd_key IS NOT NULL
  AND sls_cust_id IS NOT NULL

{% if is_incremental() %}
    AND sls_order_dt > (
        SELECT COALESCE(MAX(sls_order_dt), 0) 
        FROM {{ this }}
    )
{% endif %}