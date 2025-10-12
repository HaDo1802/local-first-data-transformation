{{
    config(
        materialized='incremental',
        unique_key=['sls_ord_num', 'sls_prd_key'],
        schema='silver',
        on_schema_change='fail',
        meta={
            'layer': 'silver',
            'source_system': 'crm',
            'data_quality': 'cleaned_and_standardized'
        }
    )
}}

SELECT
    sls_ord_num as order_number,
    sls_prd_key as product_key,
    sls_cust_id as customer_id,
    
    -- Parse and validate dates
    {{ parse_date_yyyymmdd('sls_order_dt') }} as order_date,
    {{ parse_date_yyyymmdd('sls_ship_dt') }} as ship_date,
    {{ parse_date_yyyymmdd('sls_due_dt') }} as due_date,
    
    -- Calculate and validate financial amounts
    {{ calculate_sales_amount('sls_sales', 'sls_quantity', 'sls_price') }} as sales_amount,
    sls_quantity as quantity,
    {{ calculate_unit_price('sls_sales', 'sls_quantity', 'sls_price') }} as unit_price,
    
    -- Audit columns
    dwh_create_date,
    CURRENT_TIMESTAMP as dwh_updated_at,
    dbt_run_timestamp

FROM {{ ref('bronze_crm_sales_details') }}

WHERE sls_ord_num IS NOT NULL
  AND sls_prd_key IS NOT NULL
  AND sls_cust_id IS NOT NULL
  AND sls_quantity > 0

{% if is_incremental() %}
    -- Only process new or updated records
    AND dwh_create_date > (
        SELECT COALESCE(MAX(dwh_create_date), '1900-01-01'::timestamp)
        FROM {{ this }}
    )
{% endif %}