-- Audit and logging macros for data quality monitoring

-- Macro to log model execution start
{% macro log_model_start() %}
    {% if target.name == 'prod' %}
        INSERT INTO {{ target.schema }}_audit.model_execution_log (
            model_name,
            execution_started_at,
            target_schema,
            dbt_version
        ) VALUES (
            '{{ this }}',
            CURRENT_TIMESTAMP,
            '{{ target.schema }}',
            '{{ dbt_version }}'
        )
    {% endif %}
{% endmacro %}

-- Macro to log model execution end with row count
{% macro log_model_end() %}
    {% if target.name == 'prod' %}
        UPDATE {{ target.schema }}_audit.model_execution_log 
        SET 
            execution_completed_at = CURRENT_TIMESTAMP,
            rows_affected = (SELECT COUNT(*) FROM {{ this }}),
            status = 'SUCCESS'
        WHERE model_name = '{{ this }}' 
          AND execution_completed_at IS NULL
    {% endif %}
{% endmacro %}

-- Macro to create audit schema and tables if they don't exist
{% macro create_audit_schema() %}
    {% if target.name == 'prod' %}
        CREATE SCHEMA IF NOT EXISTS {{ target.schema }}_audit;
        
        CREATE TABLE IF NOT EXISTS {{ target.schema }}_audit.model_execution_log (
            id SERIAL PRIMARY KEY,
            model_name VARCHAR(255),
            execution_started_at TIMESTAMP,
            execution_completed_at TIMESTAMP,
            target_schema VARCHAR(100),
            dbt_version VARCHAR(50),
            rows_affected INTEGER,
            status VARCHAR(20) DEFAULT 'RUNNING'
        );
        
        CREATE TABLE IF NOT EXISTS {{ target.schema }}_audit.data_quality_results (
            id SERIAL PRIMARY KEY,
            model_name VARCHAR(255),
            test_name VARCHAR(255),
            test_result VARCHAR(20),
            failure_count INTEGER,
            execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    {% endif %}
{% endmacro %}

-- Macro to generate schema name based on environment and custom logic
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- elif target.name == 'prod' -%}
        {# In production, use simple schema names (bronze, silver, gold) #}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {# In dev/staging, prefix with target schema for isolation #}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}

-- Macro to check data freshness and alert if stale
{% macro check_data_freshness(table_name, date_column, max_age_hours=24) %}
    {% set query %}
        SELECT 
            CASE 
                WHEN MAX({{ date_column }}) < CURRENT_TIMESTAMP - INTERVAL '{{ max_age_hours }} hours'
                THEN 'STALE'
                ELSE 'FRESH'
            END as freshness_status
        FROM {{ table_name }}
    {% endset %}
    
    {% if execute %}
        {% set results = run_query(query) %}
        {% set freshness = results.columns[0].values()[0] %}
        {% if freshness == 'STALE' %}
            {{ log("WARNING: Data in " ~ table_name ~ " is stale (older than " ~ max_age_hours ~ " hours)", info=true) }}
        {% endif %}
    {% endif %}
{% endmacro %}

-- Macro to validate row counts between source and target
{% macro validate_row_count(source_table, target_table, tolerance_pct=5) %}
    {% set source_count_query %}
        SELECT COUNT(*) as row_count FROM {{ source_table }}
    {% endset %}
    
    {% set target_count_query %}
        SELECT COUNT(*) as row_count FROM {{ target_table }}
    {% endset %}
    
    {% if execute %}
        {% set source_count = run_query(source_count_query).columns[0].values()[0] %}
        {% set target_count = run_query(target_count_query).columns[0].values()[0] %}
        {% set variance_pct = ((target_count - source_count) / source_count * 100) | abs %}
        
        {% if variance_pct > tolerance_pct %}
            {{ log("WARNING: Row count variance of " ~ variance_pct ~ "% exceeds tolerance of " ~ tolerance_pct ~ "%", info=true) }}
        {% endif %}
    {% endif %}
{% endmacro %}