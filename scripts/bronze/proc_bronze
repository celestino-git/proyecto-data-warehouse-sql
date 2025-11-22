USE bronze;

DROP PROCEDURE IF EXISTS load_bronze;

DELIMITER //

CREATE PROCEDURE load_bronze()
BEGIN
	-- =============================================================================
	-- STORED PROCEDURE: Load Bronze Layer (Ingesta de CSVs)
	-- =============================================================================
	-- Descripción:
	--   1. Trunca (vacía) las tablas de la capa Bronze.
	--   2. Carga masiva de datos desde archivos CSV usando LOAD DATA INFILE.
    --   3. Maneja formatos de fecha y valores nulos/vacíos.
	-- =============================================================================

	DECLARE start_time DATETIME;
	SET start_time = NOW();

	-- -----------------------------------------------------------------------------
	-- 1. CRM_CUST_INFO
	-- -----------------------------------------------------------------------------
	SELECT 'Loading bronze.crm_cust_info...' AS Status;
	TRUNCATE TABLE bronze.crm_cust_info;

	LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/WAREHOUSE PROJECT/DATASETS/source_crm/cust_info.csv' 
	INTO TABLE bronze.crm_cust_info
	FIELDS TERMINATED BY ',' 
	OPTIONALLY ENCLOSED BY '"' 
	LINES TERMINATED BY '\r\n' 
	IGNORE 1 ROWS
	(cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, @cst_create_date)
	SET 
        -- Asumiendo formato fecha estándar o manejando vacíos
		cst_create_date = NULLIF(@cst_create_date, '');

	-- -----------------------------------------------------------------------------
	-- 2. CRM_PRD_INFO
	-- -----------------------------------------------------------------------------
	SELECT 'Loading bronze.crm_prd_info...' AS Status;
	TRUNCATE TABLE bronze.crm_prd_info;

	LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/WAREHOUSE PROJECT/DATASETS/source_crm/prd_info.csv' 
	INTO TABLE bronze.crm_prd_info
	FIELDS TERMINATED BY ',' 
	OPTIONALLY ENCLOSED BY '"' 
	LINES TERMINATED BY '\r\n' 
	IGNORE 1 ROWS
	(prd_id, prd_key, prd_nm, prd_cost, prd_line, @prd_start_dt, @prd_end_dt)
	SET 
		prd_start_dt = NULLIF(@prd_start_dt, ''),
		prd_end_dt   = NULLIF(@prd_end_dt, '');

	-- -----------------------------------------------------------------------------
	-- 3. CRM_SALES_DETAILS
	-- -----------------------------------------------------------------------------
	SELECT 'Loading bronze.crm_sales_details...' AS Status;
	TRUNCATE TABLE bronze.crm_sales_details;

	LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/WAREHOUSE PROJECT/DATASETS/source_crm/sales_details.csv' 
	INTO TABLE bronze.crm_sales_details
	FIELDS TERMINATED BY ',' 
	OPTIONALLY ENCLOSED BY '"' 
	LINES TERMINATED BY '\r\n' 
	IGNORE 1 ROWS
	(sls_ord_num, sls_prd_key, sls_cust_id, @sls_order_dt, @sls_ship_dt, @sls_due_dt, sls_sales, sls_quantity, sls_price)
	SET 
        -- Nota: Como dijiste que arreglaste las fechas en Excel a YYYY-MM-DD o YYYYMMDD, 
        -- NULLIF es suficiente para evitar errores con celdas vacías.
		sls_order_dt = NULLIF(@sls_order_dt, ''),
		sls_ship_dt  = NULLIF(@sls_ship_dt, ''),
		sls_due_dt   = NULLIF(@sls_due_dt, '');

	-- -----------------------------------------------------------------------------
	-- 4. ERP_LOC_A101
	-- -----------------------------------------------------------------------------
	SELECT 'Loading bronze.erp_loc_a101...' AS Status;
	TRUNCATE TABLE bronze.erp_loc_a101;

	LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/WAREHOUSE PROJECT/DATASETS/source_erp/LOC_A101.csv' 
	INTO TABLE bronze.erp_loc_a101
	FIELDS TERMINATED BY ',' 
	OPTIONALLY ENCLOSED BY '"' 
	LINES TERMINATED BY '\r\n' 
	IGNORE 1 ROWS
	(cid, cntry);

	-- -----------------------------------------------------------------------------
	-- 5. ERP_CUST_AZ12
	-- -----------------------------------------------------------------------------
	SELECT 'Loading bronze.erp_cust_az12...' AS Status;
	TRUNCATE TABLE bronze.erp_cust_az12;

	LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/WAREHOUSE PROJECT/DATASETS/source_erp/CUST_AZ12.csv' 
	INTO TABLE bronze.erp_cust_az12
	FIELDS TERMINATED BY ',' 
	OPTIONALLY ENCLOSED BY '"' 
	LINES TERMINATED BY '\r\n' 
	IGNORE 1 ROWS
	(cid, @bdate, gen)
	SET 
		-- Transformación explícita de DD/MM/YYYY a YYYY-MM-DD
		bdate = STR_TO_DATE(@bdate, '%d/%m/%Y');

	-- -----------------------------------------------------------------------------
	-- 6. ERP_PX_CAT_G1V2
	-- -----------------------------------------------------------------------------
	SELECT 'Loading bronze.erp_px_cat_g1v2...' AS Status;
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;

	LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/WAREHOUSE PROJECT/DATASETS/source_erp/PX_CAT_G1V2.csv' 
	INTO TABLE bronze.erp_px_cat_g1v2
	FIELDS TERMINATED BY ',' 
	OPTIONALLY ENCLOSED BY '"' 
	LINES TERMINATED BY '\r\n' 
	IGNORE 1 ROWS
    (id, cat, subcat, maintenance);

	-- =============================================================================
	-- RESUMEN DE CARGA
	-- =============================================================================
    SELECT '------------------------------------------------' AS Status
    UNION ALL
    SELECT 'BRONZE LAYER LOADED SUCCESSFULLY'
    UNION ALL
    SELECT CONCAT('Execution Time: ', NOW());

	SELECT 'bronze.crm_cust_info' AS Table_Name, COUNT(*) AS Rows_Loaded FROM bronze.crm_cust_info
	UNION ALL
	SELECT 'bronze.crm_prd_info', COUNT(*) FROM bronze.crm_prd_info
	UNION ALL
	SELECT 'bronze.crm_sales_details', COUNT(*) FROM bronze.crm_sales_details
	UNION ALL
	SELECT 'bronze.erp_loc_a101', COUNT(*) FROM bronze.erp_loc_a101
	UNION ALL
	SELECT 'bronze.erp_cust_az12', COUNT(*) FROM bronze.erp_cust_az12
	UNION ALL
	SELECT 'bronze.erp_px_cat_g1v2', COUNT(*) FROM bronze.erp_px_cat_g1v2;

END //

DELIMITER ;
