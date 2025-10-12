CREATE
OR REPLACE PROCEDURE CREATE_DW_TABLES (TARGET_SCHEMA TEXT) LANGUAGE PLPGSQL AS $$
BEGIN
    -- Create schema if it doesn't exist
    EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', target_schema);

    -- crm_cust_info
    EXECUTE format('
        CREATE TABLE IF NOT EXISTS %I.crm_cust_info (
            cst_id             INTEGER,
            cst_key            VARCHAR(50),
            cst_firstname      VARCHAR(50),
            cst_lastname       VARCHAR(50),
            cst_marital_status VARCHAR(50),
            cst_gndr           VARCHAR(50),
            cst_create_date    DATE,
            dwh_create_date    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )', target_schema);

    -- crm_prd_info
    EXECUTE format('
        CREATE TABLE IF NOT EXISTS %I.crm_prd_info (
            prd_id          INTEGER,
            cat_id          VARCHAR(50),
            prd_key         VARCHAR(50),
            prd_nm          VARCHAR(50),
            prd_cost        INTEGER,
            prd_line        VARCHAR(50),
            prd_start_dt    DATE,
            prd_end_dt      DATE,
            dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )', target_schema);

    -- crm_sales_details
    EXECUTE format('
        CREATE TABLE IF NOT EXISTS %I.crm_sales_details (
            sls_ord_num     VARCHAR(50),
            sls_prd_key     VARCHAR(50),
            sls_cust_id     INTEGER,
            sls_order_dt    DATE,
            sls_ship_dt     DATE,
            sls_due_dt      DATE,
            sls_sales       INTEGER,
            sls_quantity    INTEGER,
            sls_price       INTEGER,
            dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )', target_schema);

    -- erp_loc_a101
    EXECUTE format('
        CREATE TABLE IF NOT EXISTS %I.erp_loc_a101 (
            cid             VARCHAR(50),
            cntry           VARCHAR(50),
            dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )', target_schema);

    -- erp_cust_az12
    EXECUTE format('
        CREATE TABLE IF NOT EXISTS %I.erp_cust_az12 (
            cid             VARCHAR(50),
            bdate           DATE,
            gen             VARCHAR(50),
            dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )', target_schema);

    -- erp_px_cat_g1v2
    EXECUTE format('
        CREATE TABLE IF NOT EXISTS %I.erp_px_cat_g1v2 (
            id              VARCHAR(50),
            cat             VARCHAR(50),
            subcat          VARCHAR(50),
            maintenance     VARCHAR(50),
            dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )', target_schema);

    RAISE NOTICE '✅ Tables created in schema: %', target_schema;
END;
$$;