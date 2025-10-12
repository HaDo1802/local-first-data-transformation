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
    
    -- Validate and clean birth date
    CASE
        WHEN bdate > CURRENT_DATE THEN NULL  -- Future dates are invalid
        WHEN bdate < '1900-01-01' THEN NULL  -- Very old dates are suspicious
        ELSE bdate
    END as birth_date,
    
    {{ standardize_gender('gen') }} as gender,
    
    -- Calculate age if birth date is valid
    CASE 
        WHEN bdate IS NOT NULL AND bdate <= CURRENT_DATE AND bdate > '1900-01-01'
        THEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, bdate))
        ELSE NULL
    END as age,
    
    -- Audit columns
    dwh_create_date,
    CURRENT_TIMESTAMP as dwh_updated_at,
    dbt_run_timestamp

FROM {{ ref('bronze_erp_cust_az12') }}

WHERE cid IS NOT NULL