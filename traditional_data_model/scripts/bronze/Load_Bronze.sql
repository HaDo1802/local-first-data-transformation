CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP := clock_timestamp();
    batch_end_time TIMESTAMP;
BEGIN
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '================================================';

    ------------------------------------------------
    RAISE NOTICE 'Loading CRM Tables';
    ------------------------------------------------

    -- crm_cust_info
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_cust_info;
    RAISE NOTICE '>> Inserting Into: bronze.crm_cust_info';
    COPY bronze.crm_cust_info FROM '/tmp/source_crm/cust_info.csv' WITH (FORMAT csv, HEADER true);
    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(SECOND FROM end_time - start_time);

    -- crm_prd_info
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_prd_info;
    RAISE NOTICE '>> Inserting Into: bronze.crm_prd_info';
    COPY bronze.crm_prd_info FROM '/tmp/source_crm/prd_info.csv' WITH (FORMAT csv, HEADER true);
    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(SECOND FROM end_time - start_time);

    -- crm_sales_details
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_sales_details;
    RAISE NOTICE '>> Inserting Into: bronze.crm_sales_details';
    COPY bronze.crm_sales_details FROM '/tmp/source_crm/sales_details.csv' WITH (FORMAT csv, HEADER true);
    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(SECOND FROM end_time - start_time);

    ------------------------------------------------
    RAISE NOTICE 'Loading ERP Tables';
    ------------------------------------------------

    -- erp_loc_a101
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_loc_a101;
    RAISE NOTICE '>> Inserting Into: bronze.erp_loc_a101';
    COPY bronze.erp_loc_a101 FROM '/tmp/source_erp/loc_a101.csv' WITH (FORMAT csv, HEADER true);
    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(SECOND FROM end_time - start_time);

    -- erp_cust_az12
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_cust_az12;
    RAISE NOTICE '>> Inserting Into: bronze.erp_cust_az12';
    COPY bronze.erp_cust_az12 FROM '/tmp/source_erp/cust_az12.csv' WITH (FORMAT csv, HEADER true);
    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(SECOND FROM end_time - start_time);

    -- erp_px_cat_g1v2
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    RAISE NOTICE '>> Inserting Into: bronze.erp_px_cat_g1v2';
    COPY bronze.erp_px_cat_g1v2 FROM '/tmp/source_erp/px_cat_g1v2.csv' WITH (FORMAT csv, HEADER true);
    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(SECOND FROM end_time - start_time);

    ------------------------------------------------
    batch_end_time := clock_timestamp();
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading Bronze Layer is Completed';
    RAISE NOTICE '   - Total Load Duration: % seconds', EXTRACT(SECOND FROM batch_end_time - batch_start_time);
    RAISE NOTICE '==========================================';

EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
    RAISE NOTICE 'Error Message: %', SQLERRM;
    RAISE NOTICE '==========================================';
END;
$$;


