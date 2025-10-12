-- Custom macros for data transformations and quality checks

-- Macro to standardize gender values
{% macro standardize_gender(column_name) %}
    CASE 
        WHEN UPPER(TRIM({{ column_name }})) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM({{ column_name }})) IN ('M', 'MALE') THEN 'Male'
        ELSE 'Unknown'
    END
{% endmacro %}

-- Macro to standardize marital status  
{% macro standardize_marital_status(column_name) %}
    CASE 
        WHEN UPPER(TRIM({{ column_name }})) = 'S' THEN 'Single'
        WHEN UPPER(TRIM({{ column_name }})) = 'M' THEN 'Married'
        ELSE 'Unknown'
    END
{% endmacro %}

-- Macro to standardize product line
{% macro standardize_product_line(column_name) %}
    CASE 
        WHEN UPPER(TRIM({{ column_name }})) = 'M' THEN 'Mountain'
        WHEN UPPER(TRIM({{ column_name }})) = 'R' THEN 'Road'
        WHEN UPPER(TRIM({{ column_name }})) = 'S' THEN 'Other Sales'
        WHEN UPPER(TRIM({{ column_name }})) = 'T' THEN 'Touring'
        ELSE 'Unknown'
    END
{% endmacro %}

-- Macro to parse YYYYMMDD date format
{% macro parse_date_yyyymmdd(column_name) %}
    CASE 
        WHEN {{ column_name }} IS NOT NULL AND {{ column_name }}::TEXT ~ '^\d{8}$' 
            THEN TO_DATE(TRIM({{ column_name }}::TEXT), 'YYYYMMDD')
        ELSE NULL
    END
{% endmacro %}

-- Macro to clean and trim string fields
{% macro clean_string(column_name) %}
    CASE 
        WHEN TRIM({{ column_name }}) = '' THEN NULL
        ELSE TRIM({{ column_name }})
    END
{% endmacro %}

-- Macro to extract category ID from product key
{% macro extract_category_id(product_key_column) %}
    REPLACE(SUBSTRING({{ product_key_column }} FROM 1 FOR 5), '-', '_')
{% endmacro %}

-- Macro to extract product key without category prefix
{% macro extract_product_key(product_key_column) %}
    SUBSTRING({{ product_key_column }} FROM 7)
{% endmacro %}

-- Macro to generate surrogate key
{% macro generate_surrogate_key(column_list) %}
    {{ dbt_utils.generate_surrogate_key(column_list) }}
{% endmacro %}

-- Macro to standardize country names
{% macro standardize_country(column_name) %}
    CASE
        WHEN TRIM({{ column_name }}) = 'DE' THEN 'Germany'
        WHEN TRIM({{ column_name }}) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM({{ column_name }}) = '' OR {{ column_name }} IS NULL THEN 'Unknown'
        ELSE TRIM({{ column_name }})
    END
{% endmacro %}

-- Macro to clean customer ID references  
{% macro clean_customer_id(column_name) %}
    CASE
        WHEN {{ column_name }} LIKE 'NAS%' THEN SUBSTRING({{ column_name }} FROM 4)
        ELSE REPLACE({{ column_name }}, '-', '')
    END
{% endmacro %}

-- Macro to calculate sales amount with validation
{% macro calculate_sales_amount(sales_col, quantity_col, price_col) %}
    CASE 
        WHEN {{ sales_col }} IS NULL OR {{ sales_col }} <= 0 
             OR {{ sales_col }} != {{ quantity_col }} * ABS({{ price_col }})
            THEN {{ quantity_col }} * ABS({{ price_col }})
        ELSE {{ sales_col }}
    END
{% endmacro %}

-- Macro to calculate unit price with validation
{% macro calculate_unit_price(sales_col, quantity_col, price_col) %}
    CASE 
        WHEN {{ price_col }} IS NULL OR {{ price_col }} <= 0
            THEN {{ sales_col }} / NULLIF({{ quantity_col }}, 0)
        ELSE {{ price_col }}
    END
{% endmacro %}

-- Macro to transform date from text to date timestamp
{% macro transform_date(column_name) %}
    CASE 
        WHEN {{ column_name }} IS NULL OR TRIM({{ column_name }}::TEXT) = '' THEN NULL
        WHEN {{ column_name }}::TEXT ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE({{ column_name }}::TEXT, 'YYYY-MM-DD')
        WHEN {{ column_name }}::TEXT ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE({{ column_name }}::TEXT, 'MM/DD/YYYY')
        WHEN {{ column_name }}::TEXT ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE({{ column_name }}::TEXT, 'M/D/YYYY')
        WHEN {{ column_name }}::TEXT ~ '^\d{8}$' THEN TO_DATE({{ column_name }}::TEXT, 'YYYYMMDD')
        ELSE NULL
    END
{% endmacro %}
