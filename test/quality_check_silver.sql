
/*
===============================================================================
DATA QUALITY CHECKS SCRIPT (AUDITORÍA DE CALIDAD)
===============================================================================
Propósito:
    Detectar anomalías, duplicados, valores nulos y errores de lógica en la capa 
    BRONZE antes de procesar la capa SILVER.
    
Instrucciones:
    Ejecuta cada bloque individualmente. Si obtienes resultados, esos son los 
    datos que requieren limpieza (Transformation Rules).
===============================================================================
*/

USE bronze;

-- =============================================================================
-- 1. TABLA: CRM_CUST_INFO
-- =============================================================================

-- [CHECK 1] Duplicados en la Clave Primaria (Integridad)
-- Razón: El ID de cliente debe ser único. Si sale algo aquí, necesitamos deduplicar.
SELECT 
    cst_id, 
    COUNT(*) as total_duplicados
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1;

-- [CHECK 2] Espacios en blanco no deseados (Limpieza)
-- Razón: Detectar nombres que tienen espacios al inicio o final.
SELECT cst_firstname 
FROM bronze.crm_cust_info 
WHERE LENGTH(cst_firstname) != LENGTH(TRIM(cst_firstname));

-- [CHECK 3] Consistencia de Género (Estandarización)
-- Razón: Ver qué valores extraños existen aparte de F y M.
SELECT DISTINCT cst_gndr 
FROM bronze.crm_cust_info;

-- [CHECK 4] Consistencia de Estado Civil (Estandarización)
-- Razón: Ver si hay valores distintos a S (Single) o M (Married).
SELECT DISTINCT cst_marital_status 
FROM bronze.crm_cust_info;


-- =============================================================================
-- 2. TABLA: CRM_PRD_INFO
-- =============================================================================

-- [CHECK 1] Claves Primarias Duplicadas
SELECT prd_id, COUNT(*) 
FROM bronze.crm_prd_info 
GROUP BY prd_id 
HAVING COUNT(*) > 1;

-- [CHECK 2] Formato de Clave de Producto (Integridad)
-- Razón: Verificar si las claves siguen el patrón esperado (ej. guión vs guión bajo).
SELECT prd_key 
FROM bronze.crm_prd_info 
WHERE prd_key LIKE '%-%'; -- Muestra los que tienen guión (que cambiaremos a guión bajo)

-- [CHECK 3] Valores Nulos en Costos (Integridad)
-- Razón: No podemos tener costos vacíos para cálculos financieros.
SELECT * FROM bronze.crm_prd_info 
WHERE prd_cost IS NULL OR prd_cost = '';

-- [CHECK 4] Consistencia de Fechas (Lógica de Negocio)
-- Razón: La fecha de fin no puede ser anterior a la fecha de inicio.
SELECT * FROM bronze.crm_prd_info 
WHERE prd_end_dt < prd_start_dt;


-- =============================================================================
-- 3. TABLA: CRM_SALES_DETAILS
-- =============================================================================

-- [CHECK 1] Regla de Negocio: Ventas = Cantidad * Precio
-- Razón: Detectar si el campo 'Sales' calculado en el origen está mal.
SELECT 
    sls_ord_num, 
    sls_sales, 
    sls_quantity, 
    sls_price,
    (sls_quantity * sls_price) AS calculo_teorico,
    (sls_sales - (sls_quantity * sls_price)) AS diferencia
FROM bronze.crm_sales_details
WHERE sls_sales != (sls_quantity * sls_price)
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL;

-- [CHECK 2] Valores Negativos o Cero (Validez)
-- Razón: No deberíamos tener ventas o precios negativos.
SELECT * FROM bronze.crm_sales_details
WHERE sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0;

-- [CHECK 3] Orden vs Envío (Lógica de Fechas)
-- Razón: No se puede enviar un pedido antes de que se haya realizado.
-- (Nota: Esto fallará si las fechas aún están en formato texto 'YYYYMMDD' sin convertir)
/*
SELECT * FROM bronze.crm_sales_details 
WHERE sls_order_dt > sls_ship_dt;
*/


-- =============================================================================
-- 4. TABLA: ERP_LOC_A101
-- =============================================================================

-- [CHECK 1] Formato de Clave (Integridad)
-- Razón: El ERP usa guiones (ej. 'CN-12') que deben quitarse para unir con CRM.
SELECT cid 
FROM bronze.erp_loc_a101 
WHERE cid LIKE '%-%';

-- [CHECK 2] Estandarización de Países
-- Razón: Ver cuántas formas diferentes hay de escribir "USA" o "Germany".
SELECT DISTINCT cntry 
FROM bronze.erp_loc_a101 
ORDER BY cntry;


-- =============================================================================
-- 5. TABLA: ERP_CUST_AZ12
-- =============================================================================

-- [CHECK 1] Prefijos en IDs (Limpieza)
-- Razón: Detectar IDs que empiezan con 'NAS' (legacy system artifact).
SELECT cid 
FROM bronze.erp_cust_az12 
WHERE cid LIKE 'NAS%';

-- [CHECK 2] Fechas de Nacimiento Futuras (Outliers/Errores)
-- Razón: Nadie puede haber nacido mañana.
SELECT bdate 
FROM bronze.erp_cust_az12 
WHERE bdate > NOW();

-- [CHECK 3] Estandarización de Género
-- Razón: Comparar formato con el CRM (¿usan F/M, Female/Male, 0/1?).
SELECT DISTINCT gen 
FROM bronze.erp_cust_az12;


-- =============================================================================
-- 6. TABLA: ERP_PX_CAT_G1V2
-- =============================================================================

-- [CHECK 1] Integridad Básica
-- Razón: Verificar si hay filas vacías críticas.
SELECT * FROM bronze.erp_px_cat_g1v2 
WHERE id IS NULL OR cat IS NULL;

-- [CHECK 2] Normalización de Categorías
-- Razón: Revisar si hay duplicados semánticos (ej. "Bike" vs "Bikes").
SELECT DISTINCT cat, subcat 
FROM bronze.erp_px_cat_g1v2;
