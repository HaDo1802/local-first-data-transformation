{{
    config(
        materialized='view',
        schema='gold',
        meta={
            'layer': 'gold',
            'model_type': 'dimension',
            'business_owner': 'analytics_team'
        }
    )
}}



WITH customer_base AS (
    SELECT
        cst_id,
        cst_key,
        first_name,
        last_name,
        marital_status,
        gender as crm_gender,
        create_date,
        dwh_updated_at
    FROM {{ ref('silver_crm_customers') }}
),

customer_demographics AS (
    SELECT
        customer_id,
        birth_date,
        gender as erp_gender,
        age
    FROM {{ ref('silver_erp_customer_demographics') }}
),

customer_locations AS (
    SELECT
        customer_id,
        country,
        region
    FROM {{ ref('silver_erp_locations') }}
)

SELECT
    -- Surrogate key generation
    {{ generate_surrogate_key(['cb.cst_id']) }} as customer_key,
    
    -- Natural keys
    cb.cst_id as customer_id,
    cb.cst_key as customer_number,
    
    -- Customer attributes
    cb.first_name,
    cb.last_name,
    CONCAT(
        COALESCE(cb.first_name, ''), 
        CASE WHEN cb.first_name IS NOT NULL AND cb.last_name IS NOT NULL THEN ' ' ELSE '' END,
        COALESCE(cb.last_name, '')
    ) as full_name,
    
    -- Demographics - prioritize ERP data where available
    COALESCE(cl.country, 'Unknown') as country,
    COALESCE(cl.region, 'Unknown') as region,
    cb.marital_status,
    COALESCE(cd.erp_gender, cb.crm_gender, 'Unknown') as gender,
    cd.birth_date,
    cd.age,
    
    -- Age groups for analytics
    CASE 
        WHEN cd.age IS NULL THEN 'Unknown'
        WHEN cd.age < 18 THEN 'Under 18'
        WHEN cd.age BETWEEN 18 AND 25 THEN '18-25'
        WHEN cd.age BETWEEN 26 AND 35 THEN '26-35'
        WHEN cd.age BETWEEN 36 AND 45 THEN '36-45'
        WHEN cd.age BETWEEN 46 AND 55 THEN '46-55'
        WHEN cd.age BETWEEN 56 AND 65 THEN '56-65'
        WHEN cd.age > 65 THEN 'Over 65'
        ELSE 'Unknown'
    END as age_group,
    
    -- Data quality flags
    CASE 
        WHEN cd.customer_id IS NOT NULL THEN TRUE 
        ELSE FALSE 
    END as has_demographics,
    
    CASE 
        WHEN cl.customer_id IS NOT NULL THEN TRUE 
        ELSE FALSE 
    END as has_location,
    
    -- Audit columns
    cb.create_date,
    cb.dwh_updated_at,
    CURRENT_TIMESTAMP as dwh_gold_updated_at

FROM customer_base cb
LEFT JOIN customer_demographics cd 
    ON cb.cst_key = cd.customer_id
LEFT JOIN customer_locations cl 
    ON cb.cst_key = cl.customer_id