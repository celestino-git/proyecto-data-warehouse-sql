USE silver;

DROP PROCEDURE IF EXISTS load_silver;

DELIMITER //

CREATE PROCEDURE load_silver()
BEGIN
    -- =============================================================================
    -- STORED PROCEDURE: Load Silver Layer
    -- =============================================================================
    -- Purpose:
    --   1. Drops and Re-creates Silver tables (DDL).
    --   2. Loads data from Bronze to Silver applying transformations (ETL).
    --   3. Returns a summary of loaded rows.
    -- =============================================================================

    DECLARE start_time DATETIME;
    SET start_time = NOW();

    -- -----------------------------------------------------------------------------
    -- 1. CRM_CUST_INFO
    -- -----------------------------------------------------------------------------
    SELECT 'Loading silver.crm_cust_info...' AS Status;

    DROP TABLE IF EXISTS silver.crm_cust_info;

    CREATE TABLE silver.crm_cust_info (
        cst_id             INT,
        cst_key            VARCHAR(50),
        cst_firstname      VARCHAR(50),
        cst_lastname       VARCHAR(50),
        cst_marital_status VARCHAR(50),
        cst_gndr           VARCHAR(50),
        cst_create_date    DATE,
        dwh_create_date    DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    INSERT INTO silver.crm_cust_info (
        cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date
    )
    SELECT 
        t.cst_id,
        t.cst_key,
        TRIM(t.cst_firstname) AS cst_firstname,
        TRIM(t.cst_lastname) AS cst_lastname,
        CASE 
            WHEN UPPER(TRIM(t.cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(t.cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'Not Specified'
        END AS cst_marital_status,
        CASE 
            WHEN UPPER(TRIM(t.cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(t.cst_gndr)) = 'M' THEN 'Male'
            ELSE 'Not Specified'
        END AS cst_gndr,
        t.cst_create_date
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
        FROM bronze.crm_cust_info 
    ) t
    WHERE t.flag_last = 1;

    -- -----------------------------------------------------------------------------
    -- 2. CRM_PRD_INFO
    -- -----------------------------------------------------------------------------
    SELECT 'Loading silver.crm_prd_info...' AS Status;

    DROP TABLE IF EXISTS silver.crm_prd_info;

    CREATE TABLE silver.crm_prd_info (
        prd_id          INT,
        cat_id          VARCHAR(50),
        prd_key         VARCHAR(50),
        prd_nm          VARCHAR(100),
        prd_cost        DECIMAL(10, 2),
        prd_line        VARCHAR(50),
        prd_start_dt    DATE,
        prd_end_dt      DATE,
        dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    INSERT INTO silver.crm_prd_info (
        prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
    )
    SELECT 
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, 
        SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
        prd_nm,
        IFNULL(prd_cost, 0) AS prd_cost, 
        CASE UPPER(TRIM(prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line,
        CAST(prd_start_dt AS DATE) AS prd_start_dt,
        CAST(
            DATE_SUB(
                LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt), 
                INTERVAL 1 DAY
            ) AS DATE
        ) AS prd_end_dt
    FROM bronze.crm_prd_info;

    -- -----------------------------------------------------------------------------
    -- 3. CRM_SALES_DETAILS
    -- -----------------------------------------------------------------------------
    SELECT 'Loading silver.crm_sales_details...' AS Status;

    DROP TABLE IF EXISTS silver.crm_sales_details;

    CREATE TABLE silver.crm_sales_details (
        sls_ord_num     VARCHAR(50),
        sls_prd_key     VARCHAR(50),
        sls_cust_id     INT,
        sls_order_dt    DATE,
        sls_ship_dt     DATE,
        sls_due_dt      DATE,
        sls_sales       DECIMAL(10, 2),
        sls_quantity    INT,
        sls_price       DECIMAL(10, 2),
        dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    INSERT INTO silver.crm_sales_details (
        sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
    )
    SELECT 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        CASE 
            WHEN sls_sales != sls_quantity * ABS(sls_price) OR sls_sales <= 0 OR sls_sales IS NULL 
            THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END AS sls_sales,
        sls_quantity,
        CASE 
            WHEN sls_price <= 0 OR sls_price IS NULL
            THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE sls_price
        END AS sls_price
    FROM bronze.crm_sales_details;

    -- -----------------------------------------------------------------------------
    -- 4. ERP_LOC_A101
    -- -----------------------------------------------------------------------------
    SELECT 'Loading silver.erp_loc_a101...' AS Status;

    DROP TABLE IF EXISTS silver.erp_loc_a101;

    CREATE TABLE silver.erp_loc_a101 (
        cid             VARCHAR(50),
        cntry           VARCHAR(100),
        dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    INSERT INTO silver.erp_loc_a101 (cid, cntry)
    SELECT 
        REPLACE(cid, '-', '') AS cid,
        CASE 
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END AS cntry
    FROM bronze.erp_loc_a101;

    -- -----------------------------------------------------------------------------
    -- 5. ERP_CUST_AZ12
    -- -----------------------------------------------------------------------------
    SELECT 'Loading silver.erp_cust_az12...' AS Status;

    DROP TABLE IF EXISTS silver.erp_cust_az12;

    CREATE TABLE silver.erp_cust_az12 (
        cid             VARCHAR(50),
        bdate           DATE,
        gen             VARCHAR(50),
        dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
    SELECT 
        CASE 
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
            ELSE cid
        END AS cid,
        CASE 
            WHEN bdate > NOW() THEN NULL
            ELSE bdate
        END AS bdate,
        CASE 
            WHEN UPPER(TRIM(gen)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(gen)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END AS gen
    FROM bronze.erp_cust_az12;

    -- -----------------------------------------------------------------------------
    -- 6. ERP_PX_CAT_G1V2
    -- -----------------------------------------------------------------------------
    SELECT 'Loading silver.erp_px_cat_g1v2...' AS Status;

    DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;

    CREATE TABLE silver.erp_px_cat_g1v2 (
        id              VARCHAR(50),
        cat             VARCHAR(100),
        subcat          VARCHAR(100),
        maintenance     VARCHAR(50),
        dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
    SELECT id, cat, subcat, maintenance
    FROM bronze.erp_px_cat_g1v2;

    -- =============================================================================
    -- FINAL VALIDATION & REPORTING
    -- =============================================================================
    SELECT '------------------------------------------------' AS Status
    UNION ALL
    SELECT 'ETL PROCESS COMPLETED SUCCESSFULLY'
    UNION ALL
    SELECT CONCAT('Execution Time: ', NOW());

    -- Mostrar conteo de filas cargadas
    SELECT 'silver.crm_cust_info' AS Table_Name, COUNT(*) AS Loaded_Rows FROM silver.crm_cust_info
    UNION ALL
    SELECT 'silver.crm_prd_info', COUNT(*) FROM silver.crm_prd_info
    UNION ALL
    SELECT 'silver.crm_sales_details', COUNT(*) FROM silver.crm_sales_details
    UNION ALL
    SELECT 'silver.erp_loc_a101', COUNT(*) FROM silver.erp_loc_a101
    UNION ALL
    SELECT 'silver.erp_cust_az12', COUNT(*) FROM silver.erp_cust_az12
    UNION ALL
    SELECT 'silver.erp_px_cat_g1v2', COUNT(*) FROM silver.erp_px_cat_g1v2;

END //

DELIMITER ;
