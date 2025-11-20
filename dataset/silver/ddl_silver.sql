/*
===============================================================================
DDL & ETL Script: Create and Populate Silver Tables
===============================================================================
Script Purpose:
    1. Re-define the DDL structure of all 'silver' tables (Medallion Architecture).
    2. Execute the ETL (Extract, Transform, Load) from 'bronze' to 'silver'.
    3. Apply Data Quality Rules, Normalization, and Business Logic.
    
    Database: MySQL
===============================================================================
*/

USE silver;

--------------------------------------------------------------------------------
-- 1. CRM_CUST_INFO (INFORMACIÓN DE CLIENTES LIMPIA)
--------------------------------------------------------------------------------
-- [DATA QUALITY CHECKS & ANALYSIS]
-- Antes de transformar, verificamos los problemas en Bronze:
/*
    -- 1. Check for duplicates (Data Cleansing - Duplicados)
    SELECT cst_id, COUNT(*) FROM bronze.crm_cust_info GROUP BY cst_id HAVING COUNT(*) > 1;
    
    -- 2. Check for extra spaces (Data Cleansing - Espacios)
    SELECT cst_firstname FROM bronze.crm_cust_info WHERE cst_firstname != TRIM(cst_firstname);
    
    -- 3. Check for inconsistent Marital Status/Gender (Normalization - Estandarización)
    SELECT DISTINCT cst_marital_status FROM bronze.crm_cust_info;
    SELECT DISTINCT cst_gndr FROM bronze.crm_cust_info;
*/

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
    -- [DATA CLEANSING - Espacios no deseados]
    TRIM(t.cst_firstname) AS cst_firstname,
    TRIM(t.cst_lastname) AS cst_lastname,
    
    -- [NORMALIZATION & STANDARDIZATION]
    -- Estandarización de valores S/M a Single/Married
    CASE 
        WHEN UPPER(TRIM(t.cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(t.cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'Not Specified'
    END AS cst_marital_status,
    
    -- [NORMALIZATION & STANDARDIZATION]
    -- Estandarización de valores F/M a Female/Male
    CASE 
        WHEN UPPER(TRIM(t.cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(t.cst_gndr)) = 'M' THEN 'Male'
        ELSE 'Not Specified'
    END AS cst_gndr,
    
    t.cst_create_date
FROM (
    -- [DATA CLEANSING - Eliminar Duplicados]
    -- Regla: Mantener el registro más reciente basado en cst_create_date
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info 
) t
WHERE t.flag_last = 1;


--------------------------------------------------------------------------------
-- 2. CRM_PRD_INFO (INFORMACIÓN DE PRODUCTOS LIMPIA Y ENRIQUECIDA)
--------------------------------------------------------------------------------
-- [DATA QUALITY CHECKS & ANALYSIS]
/*
    -- 1. Check for NULL costs (Data Cleansing - Valores faltantes)
    SELECT * FROM bronze.crm_prd_info WHERE prd_cost IS NULL;

    -- 2. Check Key Format Consistency for integration (Integration - Tipos de Datos)
    SELECT prd_key FROM bronze.crm_prd_info WHERE prd_key LIKE '%-%'; -- CRM usa guion
    
    -- 3. Check for invalid Date Ranges (Reglas de Negocio)
    SELECT * FROM bronze.crm_prd_info WHERE prd_end_dt < prd_start_dt;
*/

DROP TABLE IF EXISTS silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info (
    prd_id          INT,
    cat_id          VARCHAR(50), -- Change to VARCHAR to support 'CO_RF'
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
    
    -- [INTEGRATION & NORMALIZATION]
    -- Reemplazo de '-' por '_' para que coincida con el formato ERP (ej: 'CO-RF' -> 'CO_RF')
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, 
    
    -- [DERIVED COLUMNS]
    -- Extracción de la clave pura del producto (quitando prefijo de categoría)
    SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
    
    prd_nm,
    
    -- [DATA CLEANSING - Manejo de datos faltantes]
    -- Si el costo es nulo, asumimos 0 para evitar errores de cálculo
    IFNULL(prd_cost, 0) AS prd_cost, 
    
    -- [NORMALIZATION & STANDARDIZATION]
    -- Expandir códigos a nombres descriptivos
    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'T' THEN 'Touring'
        ELSE 'n/a'
    END AS prd_line,
    
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    
    -- [BUSINESS RULES & LOGIC - SCD Type 2]
    -- Calculamos la fecha fin histórica basándonos en el inicio del siguiente registro
    CAST(
        DATE_SUB(
            LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt), 
            INTERVAL 1 DAY
        ) AS DATE
    ) AS prd_end_dt
FROM bronze.crm_prd_info;


--------------------------------------------------------------------------------
-- 3. CRM_SALES_DETAILS (TRANSACCIONES DE VENTA)
--------------------------------------------------------------------------------
-- [DATA QUALITY CHECKS & ANALYSIS]
/*
    -- 1. Check Business Rule: Sales = Quantity * Price (Reglas de Negocio)
    -- Detectar inconsistencias matemáticas
    SELECT sls_ord_num, sls_sales, sls_quantity, sls_price
    FROM bronze.crm_sales_details
    WHERE sls_sales != sls_quantity * sls_price
       OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL;
       
    -- 2. Check for Negative or Zero Values (Valores Inválidos)
    SELECT * FROM bronze.crm_sales_details
    WHERE sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0;
    
    -- 3. Check Invalid Dates (already handled in Excel as per user note, but query remains for audit)
    -- SELECT * FROM bronze.crm_sales_details WHERE LENGTH(sls_order_dt) != 8;
*/

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
    
    -- [BUSINESS RULES & LOGIC - Recálculo de Ventas]
    -- Regla: Si Ventas es inconsistente, negativo o nulo, recalcular como Cantidad * ABS(Precio)
    CASE 
        WHEN sls_sales != sls_quantity * ABS(sls_price) 
             OR sls_sales <= 0 
             OR sls_sales IS NULL 
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,
    
    sls_quantity,
    
    -- [BUSINESS RULES & LOGIC - Recálculo de Precio]
    -- Regla: Si Precio es negativo o nulo, derivarlo de Ventas / Cantidad. Evitar div/0 con NULLIF.
    CASE 
        WHEN sls_price <= 0 OR sls_price IS NULL
        THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END AS sls_price

FROM bronze.crm_sales_details;


--------------------------------------------------------------------------------
-- 4. ERP_LOC_A101 (LOCALIZACIÓN ERP)
--------------------------------------------------------------------------------
-- [DATA QUALITY CHECKS & ANALYSIS]
/*
    -- 1. Integration Check: Verify key format differences (Integración)
    SELECT cid FROM bronze.erp_loc_a101 WHERE cid LIKE '%-%';
    
    -- 2. Standardization Check: Country codes vs Names (Normalización)
    SELECT DISTINCT cntry FROM bronze.erp_loc_a101 ORDER BY cntry;
*/

DROP TABLE IF EXISTS silver.erp_loc_a101;

CREATE TABLE silver.erp_loc_a101 (
    cid             VARCHAR(50),
    cntry           VARCHAR(100),
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO silver.erp_loc_a101 (cid, cntry)
SELECT 
    -- [INTEGRATION - Limpieza de Claves]
    -- Eliminar guiones para que el CID coincida con el formato CRM (Integration Key)
    REPLACE(cid, '-', '') AS cid,
    
    -- [NORMALIZATION & STANDARDIZATION]
    -- Unificar códigos de país (DE -> Germany, US/USA -> United States) y manejar nulos
    CASE 
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(cntry)
    END AS cntry
FROM bronze.erp_loc_a101;


--------------------------------------------------------------------------------
-- 5. ERP_CUST_AZ12 (INFO ADICIONAL DE CLIENTE ERP)
--------------------------------------------------------------------------------
-- [DATA QUALITY CHECKS & ANALYSIS]
/*
    -- 1. Check for ID prefixes (Integration - Formato de ID)
    SELECT cid FROM bronze.erp_cust_az12 WHERE cid LIKE 'NAS%';
    
    -- 2. Check for Future Birthdates (Valores Inválidos / Outliers)
    SELECT bdate FROM bronze.erp_cust_az12 WHERE bdate > NOW();
    
    -- 3. Check Gender format (Normalización)
    SELECT DISTINCT gen FROM bronze.erp_cust_az12;
*/

DROP TABLE IF EXISTS silver.erp_cust_az12;

CREATE TABLE silver.erp_cust_az12 (
    cid             VARCHAR(50),
    bdate           DATE,
    gen             VARCHAR(50),
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
SELECT 
    -- [INTEGRATION - Limpieza de Prefijos]
    -- Si el ID comienza con 'NAS', eliminamos esos 3 caracteres para obtener el ID limpio
    CASE 
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
        ELSE cid
    END AS cid,
    
    -- [DATA CLEANSING - Valores Inválidos]
    -- Regla: Si la fecha de nacimiento es futura, convertir a NULL
    CASE 
        WHEN bdate > NOW() THEN NULL
        ELSE bdate
    END AS bdate,
    
    -- [NORMALIZATION & STANDARDIZATION]
    -- Estandarización de Género
    CASE 
        WHEN UPPER(TRIM(gen)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(gen)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END AS gen
FROM bronze.erp_cust_az12;


--------------------------------------------------------------------------------
-- 6. ERP_PX_CAT_G1V2 (CATEGORÍAS DE PRODUCTO ERP)
--------------------------------------------------------------------------------
-- [DATA QUALITY CHECKS & ANALYSIS]
/*
    -- 1. Basic Quality Check (Validación General)
    -- Verificamos que no haya nulos críticos en claves
    SELECT * FROM bronze.erp_px_cat_g1v2 WHERE id IS NULL;
*/

DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;

CREATE TABLE silver.erp_px_cat_g1v2 (
    id              VARCHAR(50),
    cat             VARCHAR(100),
    subcat          VARCHAR(100),
    maintenance     VARCHAR(50),
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
SELECT 
    -- [PASSTHROUGH]
    -- La calidad de datos en origen es alta; ingestión directa sin transformaciones complejas.
    id,
    cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2;

--------------------------------------------------------------------------------
-- ####### VERIFICACIONES FINALES #######
--------------------------------------------------------------------------------
SELECT 'Crm Cust Info (Silver)' AS Tabla, COUNT(*) AS Total_Filas FROM silver.crm_cust_info
UNION ALL
SELECT 'Crm Prd Info (Silver)' AS Tabla, COUNT(*) AS Total_Filas FROM silver.crm_prd_info
UNION ALL
SELECT 'Crm Sales Details (Silver)' AS Tabla, COUNT(*) AS Total_Filas FROM silver.crm_sales_details
UNION ALL
SELECT 'Erp Loc A101 (Silver)' AS Tabla, COUNT(*) AS Total_Filas FROM silver.erp_loc_a101
UNION ALL
SELECT 'Erp Cust AZ12 (Silver)' AS Tabla, COUNT(*) AS Total_Filas FROM silver.erp_cust_az12
UNION ALL
SELECT 'Erp Px Cat G1V2 (Silver)' AS Tabla, COUNT(*) AS Total_Filas FROM silver.erp_px_cat_g1v2;
