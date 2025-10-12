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
    {{ clean_customer_id('cid') }} as customer_id,
    {{ standardize_country('cntry') }} as country,
    
    -- Add region classification based on country
    CASE
        WHEN {{ standardize_country('cntry') }} IN ('United States', 'Canada', 'Mexico') THEN 'North America'
        WHEN {{ standardize_country('cntry') }} IN ('Germany', 'France', 'United Kingdom', 'Italy', 'Spain') THEN 'Europe'
        WHEN {{ standardize_country('cntry') }} IN ('Australia', 'New Zealand') THEN 'Oceania'
        WHEN {{ standardize_country('cntry') }} IN ('Japan', 'China', 'India', 'South Korea') THEN 'Asia'
        ELSE 'Other'
    END as region,
    
    -- Audit columns
    dwh_create_date,
    CURRENT_TIMESTAMP as dwh_updated_at,
    dbt_run_timestamp

FROM {{ ref('bronze_erp_loc_a101') }}

WHERE cid IS NOT NULL