{{
    config(
        materialized='table',
        schema='silver',
        meta={
            'layer': 'silver',
            'source_system': 'erp',
            'data_quality': 'cleaned_and_standardized'
        }
    )
}}

SELECT
    id as category_id,
    {{ clean_string('cat') }} as category,
    {{ clean_string('subcat') }} as subcategory,
    
    -- Standardize maintenance flag
    CASE
        WHEN UPPER(TRIM(maintenance)) IN ('YES', 'Y', '1', 'TRUE') THEN 'Yes'
        WHEN UPPER(TRIM(maintenance)) IN ('NO', 'N', '0', 'FALSE') THEN 'No'
        ELSE 'Unknown'
    END as maintenance_required,
    
    -- Audit columns
    dwh_create_date,
    CURRENT_TIMESTAMP as dwh_updated_at,
    dbt_run_timestamp

FROM {{ ref('bronze_erp_px_cat_g1v2') }}

WHERE id IS NOT NULL