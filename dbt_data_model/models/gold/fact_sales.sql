{{
    config(
        materialized='table',
        schema='gold',
        meta={
            'layer': 'gold',
            'model_type': 'fact',
            'business_owner': 'analytics_team'
        }
    )
}}


WITH sales_base AS (
    SELECT
        order_number,
        product_key,
        customer_id,
        order_date,
        ship_date,
        due_date,
        sales_amount,
        quantity,
        unit_price,
        dwh_updated_at
    FROM {{ ref('silver_crm_sales') }}
),

customer_dim AS (
    SELECT 
        customer_id,
        customer_key
    FROM {{ ref('dim_customers') }}
),

product_dim AS (
    SELECT 
        product_number,
        product_key
    FROM {{ ref('dim_products') }}
),

date_dim AS (
    SELECT 
        date_actual,
        date_key
    FROM {{ ref('dim_date') }}
)

SELECT
    -- Fact table surrogate key
    {{ generate_surrogate_key(['sb.order_number', 'sb.product_key']) }} as sales_key,
    
    -- Business keys
    sb.order_number,
    
    -- Foreign keys to dimensions
    COALESCE(cd.customer_key, 'UNKNOWN_CUSTOMER') as customer_key,
    COALESCE(pd.product_key, 'UNKNOWN_PRODUCT') as product_key,
    COALESCE(order_dd.date_key, 'UNKNOWN_DATE') as order_date_key,
    COALESCE(ship_dd.date_key, 'UNKNOWN_DATE') as ship_date_key,
    COALESCE(due_dd.date_key, 'UNKNOWN_DATE') as due_date_key,
    
    -- Actual dates (for convenience)
    sb.order_date,
    sb.ship_date,
    sb.due_date,
    
    -- Measures (additive facts)
    sb.sales_amount,
    sb.quantity,
    sb.unit_price,
    
    -- Calculated measures
    sb.sales_amount * sb.quantity as total_revenue,
    CASE 
        WHEN sb.quantity > 0 THEN sb.sales_amount / sb.quantity 
        ELSE 0 
    END as average_unit_price,
    
    -- Date calculations for analytics
    CASE 
        WHEN sb.ship_date IS NOT NULL AND sb.order_date IS NOT NULL 
        THEN sb.ship_date - sb.order_date 
        ELSE NULL 
    END as days_to_ship,
    
    CASE 
        WHEN sb.due_date IS NOT NULL AND sb.order_date IS NOT NULL 
        THEN sb.due_date - sb.order_date 
        ELSE NULL 
    END as days_to_due,
    
    CASE 
        WHEN sb.ship_date IS NOT NULL AND sb.due_date IS NOT NULL 
        THEN CASE 
            WHEN sb.ship_date <= sb.due_date THEN 'On Time'
            ELSE 'Late'
        END
        ELSE 'Unknown'
    END as delivery_status,
    
    -- Data quality flags
    CASE 
        WHEN cd.customer_key IS NOT NULL THEN TRUE 
        ELSE FALSE 
    END as has_valid_customer,
    
    CASE 
        WHEN pd.product_key IS NOT NULL THEN TRUE 
        ELSE FALSE 
    END as has_valid_product,
    
    CASE 
        WHEN sb.ship_date IS NOT NULL THEN TRUE 
        ELSE FALSE 
    END as has_ship_date,
    
    -- Audit columns
    sb.dwh_updated_at,
    CURRENT_TIMESTAMP as dwh_gold_updated_at

FROM sales_base sb

-- Join to dimension tables for foreign keys
LEFT JOIN customer_dim cd 
    ON sb.customer_id = cd.customer_id

LEFT JOIN product_dim pd 
    ON sb.product_key = pd.product_number

LEFT JOIN date_dim order_dd 
    ON sb.order_date = order_dd.date_actual

LEFT JOIN date_dim ship_dd 
    ON sb.ship_date = ship_dd.date_actual

LEFT JOIN date_dim due_dd 
    ON sb.due_date = due_dd.date_actual

-- Data quality filters
WHERE sb.order_number IS NOT NULL
  AND sb.product_key IS NOT NULL
  AND sb.customer_id IS NOT NULL
  AND sb.quantity > 0
  AND sb.sales_amount >= 0