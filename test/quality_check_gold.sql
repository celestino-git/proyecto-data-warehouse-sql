/*
===============================================================================
DATA QUALITY CHECKS - GOLD LAYER
===============================================================================
Propósito:
    Validar la integridad, unicidad y lógica de negocio de las vistas finales
    (Dimensiones y Hechos) antes de conectar Power BI o herramientas de reporte.
===============================================================================
*/

USE gold;

-- =============================================================================
-- 1. VERIFICACIÓN: DIM_CUSTOMERS (Clientes)
-- =============================================================================

-- [CHECK 1] Unicidad de la Clave Subrogada (Debe devolver 0 filas)
-- Razón: La customer_key es nuestra PK en el DWH, no puede repetirse.
SELECT customer_key, COUNT(*) 
FROM gold.dim_customers 
GROUP BY customer_key 
HAVING COUNT(*) > 1;

-- [CHECK 2] Unicidad de la Clave de Negocio (Debe devolver 0 filas)
-- Razón: Ya limpiamos duplicados en Silver. Si sale algo aquí, la deduplicación falló.
SELECT customer_number, COUNT(*) 
FROM gold.dim_customers 
GROUP BY customer_number 
HAVING COUNT(*) > 1;

-- [CHECK 3] Distribución de Género (Informativo)
-- Razón: Verificar que la lógica de integración (CRM vs ERP) funcionó y no todo es n/a.
SELECT gender, COUNT(*) AS total
FROM gold.dim_customers
GROUP BY gender;


-- =============================================================================
-- 2. VERIFICACIÓN: DIM_PRODUCTS (Productos)
-- =============================================================================

-- [CHECK 1] Unicidad de la Clave Subrogada (Debe devolver 0 filas)
SELECT product_key, COUNT(*) 
FROM gold.dim_products 
GROUP BY product_key 
HAVING COUNT(*) > 1;

-- [CHECK 2] Unicidad del Código de Producto (Debe devolver 0 filas)
-- Razón: Como filtramos "end_date IS NULL", solo debería haber UNA versión activa de cada producto.
SELECT product_number, COUNT(*) 
FROM gold.dim_products 
GROUP BY product_number 
HAVING COUNT(*) > 1;

-- [CHECK 3] Categorías Faltantes (Informativo)
-- Razón: Ver si el cruce con categorías (px_cat_g1v2) dejó muchos productos sin clasificar.
SELECT category_name, COUNT(*) 
FROM gold.dim_products 
GROUP BY category_name;


-- =============================================================================
-- 3. VERIFICACIÓN: FACT_SALES (Ventas) - ¡LA MÁS IMPORTANTE!
-- =============================================================================

-- [CHECK 1] Integridad Referencial - PRODUCTOS HUÉRFANOS
-- Razón: Detectar ventas de productos que NO existen en la dimensión productos.
-- NOTA: Aquí ES DONDE ESPERAMOS VER RESULTADOS (los "missing products" que descubrimos antes).
SELECT 
    'Productos Faltantes' AS Problema,
    COUNT(*) AS Total_Filas_Afectadas
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p ON f.product_key = p.product_key
WHERE p.product_key IS NULL;

-- [CHECK 2] Integridad Referencial - CLIENTES HUÉRFANOS
-- Razón: Detectar ventas a clientes que NO existen en la dimensión clientes.
-- Esto debería dar 0, ya que arreglamos la duplicidad de clientes.
SELECT 
    'Clientes Faltantes' AS Problema,
    COUNT(*) AS Total_Filas_Afectadas
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c ON f.customer_key = c.customer_key
WHERE c.customer_key IS NULL;

-- [CHECK 3] Lógica de Fechas (Validez)
-- Razón: No se puede enviar un pedido antes de que se haya realizado.
SELECT * FROM gold.fact_sales
WHERE shipping_date < order_date;

-- [CHECK 4] Validez de Datos Numéricos
-- Razón: Ventas negativas o cero (a menos que sean devoluciones, pero aquí filtramos en Silver).
SELECT * FROM gold.fact_sales
WHERE sales_amount <= 0 OR quantity <= 0 OR price <= 0;
