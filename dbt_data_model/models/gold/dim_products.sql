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



WITH product_base AS (
    SELECT
        prd_id,
        category_id,
        product_key,
        product_name,
        cost,
        product_line,
        start_date,
        end_date,
        dwh_updated_at
    FROM {{ ref('silver_crm_products') }}
),

product_categories AS (
    SELECT
        category_id,
        category,
        subcategory,
        maintenance_required
    FROM {{ ref('silver_erp_product_categories') }}
)

SELECT
    -- Surrogate key generation
    {{ generate_surrogate_key(['pb.prd_id']) }} as product_key,
    
    -- Natural keys
    pb.prd_id as product_id,
    pb.product_key as product_number,
    
    -- Product attributes
    pb.product_name,
    pb.cost,
    pb.product_line,
    
    -- Category information from ERP
    pb.category_id,
    COALESCE(pc.category, 'Unknown') as category,
    COALESCE(pc.subcategory, 'Unknown') as subcategory,
    COALESCE(pc.maintenance_required, 'Unknown') as maintenance_required,
    
    -- Product lifecycle
    pb.start_date,
    pb.end_date,
    CASE 
        WHEN pb.end_date IS NULL OR pb.end_date > CURRENT_DATE THEN 'Active'
        ELSE 'Discontinued'
    END as product_status,
    
    -- Cost categories for analytics
    CASE 
        WHEN pb.cost = 0 THEN 'No Cost'
        WHEN pb.cost BETWEEN 1 AND 50 THEN 'Low Cost (1-50)'
        WHEN pb.cost BETWEEN 51 AND 200 THEN 'Medium Cost (51-200)'
        WHEN pb.cost BETWEEN 201 AND 500 THEN 'High Cost (201-500)'
        WHEN pb.cost > 500 THEN 'Premium (500+)'
        ELSE 'Unknown'
    END as cost_category,
    
    -- Product line grouping
    CASE 
        WHEN pb.product_line IN ('Mountain', 'Road', 'Touring') THEN 'Bikes'
        WHEN pb.product_line = 'Other Sales' THEN 'Accessories'
        ELSE 'Other'
    END as product_group,
    
    -- Data quality flags
    CASE 
        WHEN pc.category_id IS NOT NULL THEN TRUE 
        ELSE FALSE 
    END as has_category_info,
    
    -- Audit columns
    pb.dwh_updated_at,
    CURRENT_TIMESTAMP as dwh_gold_updated_at

FROM product_base pb
LEFT JOIN product_categories pc 
    ON pb.category_id = pc.category_id