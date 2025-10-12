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


WITH cleaned_products AS (
    SELECT
        prd_id,
        -- Extract category ID from product key
        {{ extract_category_id('prd_key') }} as category_id,
        -- Clean product key by removing category prefix
        {{ extract_product_key('prd_key') }} as product_key,
        {{ clean_string('prd_nm') }} as product_name,
        COALESCE(prd_cost, 0) as cost,
        {{ standardize_product_line('prd_line') }} as product_line,
        prd_start_dt as start_date,
        dwh_create_date,
        dbt_run_timestamp,
        -- Calculate end date using window function (next product version start date - 1 day)
        LEAD(prd_start_dt) OVER (
            PARTITION BY {{ extract_product_key('prd_key') }}
            ORDER BY prd_start_dt
        ) - INTERVAL '1 day' as calculated_end_date,
        prd_end_dt as original_end_date
    
    FROM {{ ref('bronze_crm_prd_info') }}
    
    WHERE prd_id IS NOT NULL
      AND prd_key IS NOT NULL
)

SELECT
    prd_id,
    category_id,
    product_key,
    product_name,
    cost,
    product_line,
    start_date,
    -- Use calculated end date if original is null, otherwise use original
    COALESCE(original_end_date, calculated_end_date) as end_date,
    dwh_create_date,
    CURRENT_TIMESTAMP as dwh_updated_at,
    dbt_run_timestamp
    
FROM cleaned_products