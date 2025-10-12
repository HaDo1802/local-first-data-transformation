{{
    config(
        materialized='table',
        schema='silver',
        meta={
            'layer': 'silver',
            'source_system': 'crm',
            'data_quality': 'cleaned_and_standardized'
        }
    )
}}

WITH cleaned_customers AS (
    SELECT
        cst_id,
        cst_key,
        {{ clean_string('cst_firstname') }} as first_name,
        {{ clean_string('cst_lastname') }} as last_name,
        {{ standardize_marital_status('cst_marital_status') }} as marital_status,
        {{ standardize_gender('cst_gndr') }} as gender,
        cst_create_date as create_date,
        dwh_create_date,
        dbt_run_timestamp,
        -- Add row number for deduplication (keep latest record per customer)
        ROW_NUMBER() OVER (
            PARTITION BY cst_id 
            ORDER BY cst_create_date DESC, dwh_create_date DESC
        ) as row_num
    
    FROM {{ ref('bronze_crm_cust_info') }}
    
    WHERE cst_id IS NOT NULL
      AND cst_key IS NOT NULL
      AND cst_create_date IS NOT NULL
)

SELECT
    cst_id,
    cst_key,
    first_name,
    last_name,
    marital_status,
    gender,
    create_date,
    dwh_create_date,
    CURRENT_TIMESTAMP as dwh_updated_at,
    dbt_run_timestamp
    
FROM cleaned_customers
WHERE row_num = 1  -- Keep only the latest record per customer