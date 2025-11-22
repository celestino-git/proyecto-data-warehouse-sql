-- ################## DIM CUSTOMERS #######################
/*
-comprobación de cst_id duplicados:
select cst_id, count(*) from 
(
SELECT 
ci.cst_id,
ci.cst_key,
ci.cst_firstname,
ci.cst_lastname, 
ci.cst_marital_status,
ci.cst_gndr,
ci.cst_create_date,
ca.bdate,
ca.gen,
la.cntry
FROM silver.crm_cust_info as ci
left join silver.erp_cust_az12 ca
	on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
	on ci.cst_key = la.cid)t 
group by cst_id
having count(*)>1; 	

LIMPIEZA: 
-- PASO 1: Añadir una columna temporal con ID único
ALTER TABLE silver.erp_loc_a101 ADD COLUMN temp_id INT AUTO_INCREMENT PRIMARY KEY;

-- PASO 2: Borrar los duplicados manteniendo el que tenga el ID más alto
DELETE FROM silver.erp_loc_a101 
WHERE temp_id NOT IN (
    SELECT max_id FROM (
        SELECT MAX(temp_id) as max_id
        FROM silver.erp_loc_a101
        GROUP BY cid -- Agrupamos por la clave de negocio que tiene duplicados
    ) as keep_rows
);

-- PASO 3: Eliminar la columna temporal para dejar la tabla como estaba
ALTER TABLE silver.erp_loc_a101 DROP COLUMN temp_id;

*/
    
/*
integración de las dos columnas de género (gen y cst_gen):
SELECT distinct
ci.cst_gndr,
ca.gen,
la.cntry
FROM silver.crm_cust_info as ci
left join silver.erp_cust_az12 ca
	on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
	on ci.cst_key = la.cid
oeswe by 1,2;
*/

/*
SELECT distinct
ci.cst_gndr,
ca.gen,
case	when ci.cst_gndr != 'n/a' then ci.cst_gndr -- usar la infomración de la tabla maestro del crm no del erp
		else coalesce(ca.gen, 'n/a') -- coalesce devuelve el primer valor no nulo de la lista del argumento pasado
end as new_gen	
FROM silver.crm_cust_info as ci
left join silver.erp_cust_az12 ca
	on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
	on ci.cst_key = la.cid
order by 1,2;
*/

/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    Create a view for the Dimension Customers (dim_customers) that integrates
    data from CRM and ERP systems.
    
    Transformations:
    1. Surrogate Key: Auto-generated unique identifier using ROW_NUMBER().
    2. Gender Integration: Prioritize CRM gender; fallback to ERP gender.
    3. Column Renaming: Use business-friendly names (CamelCase or SnakeCase).
===============================================================================
*/

USE gold; -- Asegúrate de estar en el esquema correcto

DROP VIEW IF EXISTS gold.dim_customers;

CREATE VIEW gold.dim_customers AS
SELECT 
    -- 1. Surrogate Key (Clave única generada para el DWH)
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,
    
    -- 2. IDs Originales
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    
    -- 3. Datos Personales
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS country,
    ci.cst_marital_status AS marital_status,
    
    -- 4. Lógica de Integración de Género (Tu lógica estaba perfecta)
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- Prioridad al CRM (Maestro)
        ELSE COALESCE(ca.gen, 'n/a')               -- Fallback al ERP
    END AS gender,
    
    -- 5. Fechas
    ca.bdate AS birthdate,    
    ci.cst_create_date AS create_date

FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;












-- ################## DIM PRODUCTS #######################
-- comprobar si hay duplicados:
/*
select prd_key, count(*) 
from (
select 
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt,
pc.cat,
pc.subcat,
pc.maintenance
from silver.crm_prd_info pn	
left join silver.erp_px_cat_g1v2 pc
	on pn.cat_id=pc.id
-- si la fecha final es null significa que es un producto actual que está bn, lo que necesitamos es deshacernos de los que tienen fecha
where prd_end_dt is null)t group by prd_key
having count(*)>1;
*/



/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    Create a view for the Dimension Customers (dim_customers) that integrates
    data from CRM and ERP systems.
    
    Transformations:
    1. Surrogate Key: Auto-generated unique identifier using ROW_NUMBER().
    2. Gender Integration: Prioritize CRM gender; fallback to ERP gender.
    3. Column Renaming: Use business-friendly names (CamelCase or SnakeCase).
===============================================================================
*/

USE gold; -- Asegúrate de estar en el esquema correcto

DROP VIEW IF EXISTS gold.dim_products;
CREATE VIEW gold.dim_products as
select 
row_number() over (order by pn.prd_start_dt, pn.prd_key) product_key,
pn.prd_id product_id,
pn.prd_key product_number,
pn.prd_nm product_name,
pn.cat_id cateegory_id,
pc.cat category_name,
pc.subcat subcategory,
pc.maintenance,
pn.prd_cost cost,
pn.prd_line product_line,
pn.prd_start_dt start_time

from silver.crm_prd_info pn	
left join silver.erp_px_cat_g1v2 pc
	on pn.cat_id=pc.id;












-- ################## FACT SALES #######################
		USE gold; -- Asegúrate de estar en el esquema correcto

		DROP VIEW IF EXISTS gold.fact_sales;

		CREATE VIEW gold.fact_sales as
		select
		-- order schema: dimensions, dates, measures
		sd.sls_ord_num order_number,
		pr.product_key, -- de la view de producto
		cu.customer_key,
		sd.sls_order_dt order_date,
		sd.sls_ship_dt shipping_date,
		sd.sls_due_dt due_date,
		sd.sls_sales sales,
		sd.sls_quantity,
		sd.sls_price price
		from silver.crm_sales_details sd
		-- usar las llaves dimensionales surrogate creadas en las tablas dimensionales de las views creadas
		inner join gold.dim_products pr -- el código original pone con left joins, aquí pondré con inner para no tener ventas de productos no registrados 
			on sd.sls_prd_key=pr.product_number
		inner join gold.dim_customers cu
			on sd.sls_cust_id=cu.customer_id;
			
		-- comprobación de integridad:
		select *
		from gold.fact_sales f 
		inner join gold.dim_customers c
			on c.customer_key=f.customer_key
		inner join gold.dim_products p
			on p.product_key=f.product_key
		where p.product_key is null;
        
