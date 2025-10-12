{{
    config(
        materialized='table',
        schema='gold',
        meta={
            'layer': 'gold',
            'model_type': 'dimension',
            'business_owner': 'analytics_team'
        }
    )
}}


WITH date_spine AS (
    SELECT 
        date_day
    FROM (
        {{ dbt_utils.date_spine(
            datepart="day",
            start_date="cast('" ~ var('start_date') ~ "' as date)",
            end_date="cast('" ~ var('end_date') ~ "' as date)"
        ) }}
    )
)

SELECT
    -- Surrogate key
    {{ generate_surrogate_key(['date_day']) }} as date_key,
    
    -- Natural key
    date_day as date_actual,
    
    -- Date components
    EXTRACT(YEAR FROM date_day) as year,
    EXTRACT(QUARTER FROM date_day) as quarter,
    EXTRACT(MONTH FROM date_day) as month,
    EXTRACT(WEEK FROM date_day) as week,
    EXTRACT(DAY FROM date_day) as day,
    EXTRACT(DOW FROM date_day) as day_of_week,
    EXTRACT(DOY FROM date_day) as day_of_year,
    
    -- Date labels
    TO_CHAR(date_day, 'YYYY') as year_label,
    TO_CHAR(date_day, 'YYYY-Q"Q"') as quarter_label,
    TO_CHAR(date_day, 'YYYY-MM') as month_label,
    TO_CHAR(date_day, 'YYYY-"W"WW') as week_label,
    TO_CHAR(date_day, 'Month') as month_name,
    TO_CHAR(date_day, 'Day') as day_name,
    TO_CHAR(date_day, 'Mon') as month_name_short,
    TO_CHAR(date_day, 'Dy') as day_name_short,
    
    -- Business date flags
    CASE 
        WHEN EXTRACT(DOW FROM date_day) IN (0, 6) THEN FALSE 
        ELSE TRUE 
    END as is_weekday,
    
    CASE 
        WHEN EXTRACT(DOW FROM date_day) IN (0, 6) THEN TRUE 
        ELSE FALSE 
    END as is_weekend,
    
    CASE 
        WHEN EXTRACT(DAY FROM date_day) = 1 THEN TRUE 
        ELSE FALSE 
    END as is_month_start,
    
    CASE 
        WHEN date_day = (DATE_TRUNC('month', date_day) + INTERVAL '1 month - 1 day')::DATE 
        THEN TRUE 
        ELSE FALSE 
    END as is_month_end,
    
    CASE 
        WHEN EXTRACT(MONTH FROM date_day) IN (1, 4, 7, 10) AND EXTRACT(DAY FROM date_day) = 1 
        THEN TRUE 
        ELSE FALSE 
    END as is_quarter_start,
    
    CASE 
        WHEN EXTRACT(MONTH FROM date_day) IN (3, 6, 9, 12) 
             AND date_day = (DATE_TRUNC('quarter', date_day) + INTERVAL '3 months - 1 day')::DATE 
        THEN TRUE 
        ELSE FALSE 
    END as is_quarter_end,
    
    -- Relative date calculations
    CASE 
        WHEN date_day = CURRENT_DATE THEN TRUE 
        ELSE FALSE 
    END as is_today,
    
    CASE 
        WHEN date_day = CURRENT_DATE - INTERVAL '1 day' THEN TRUE 
        ELSE FALSE 
    END as is_yesterday,
    
    CASE 
        WHEN date_day >= DATE_TRUNC('week', CURRENT_DATE) 
             AND date_day < DATE_TRUNC('week', CURRENT_DATE) + INTERVAL '7 days'
        THEN TRUE 
        ELSE FALSE 
    END as is_current_week,
    
    CASE 
        WHEN date_day >= DATE_TRUNC('month', CURRENT_DATE) 
             AND date_day < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'
        THEN TRUE 
        ELSE FALSE 
    END as is_current_month,
    
    CASE 
        WHEN date_day >= DATE_TRUNC('quarter', CURRENT_DATE) 
             AND date_day < DATE_TRUNC('quarter', CURRENT_DATE) + INTERVAL '3 months'
        THEN TRUE 
        ELSE FALSE 
    END as is_current_quarter,
    
    CASE 
        WHEN EXTRACT(YEAR FROM date_day) = EXTRACT(YEAR FROM CURRENT_DATE) 
        THEN TRUE 
        ELSE FALSE 
    END as is_current_year,
    
    -- Audit columns
    CURRENT_TIMESTAMP as dwh_gold_updated_at

FROM date_spine