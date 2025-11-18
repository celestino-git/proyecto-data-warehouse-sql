# SQL Data Warehouse - Proyecto End-to-End 🏗️📊

Este repositorio documenta la construcción de un **Data Warehouse Moderno** desde cero utilizando **SQL Server**. El proyecto sigue las mejores prácticas de ingeniería de datos, implementando una arquitectura por capas (Medallion Architecture) y procesos ETL robustos para transformar datos crudos en insights de negocio listos para el análisis.

Basado en el tutorial de [Data with Baraa](https://www.youtube.com/watch?v=9GVqKuTVANE).

---

## 📋 Descripción del Proyecto

El objetivo principal es consolidar datos de ventas dispersos de dos sistemas fuente distintos (**ERP** y **CRM**) en un almacén de datos centralizado. Esto permite a los analistas de negocio realizar reportes históricos, análisis de tendencias y tomar decisiones basadas en una "única fuente de verdad".

### Objetivos Clave:
* **Ingesta de Datos:** Cargar datos desde archivos CSV (simulando exportaciones de sistemas) a SQL Server.
* **Arquitectura de Medallón:** Implementar capas Bronce, Plata y Oro.
* **Limpieza y Calidad (Data Quality):** Normalizar, limpiar y enriquecer datos.
* **Modelado Dimensional:** Crear un esquema de estrella (Star Schema) optimizado para BI.
* **Historización:** Manejo de dimensiones cambiantes (SCD) y preservación de datos históricos.

---

## 🏗️ Arquitectura de Datos

El proyecto utiliza la **Arquitectura Medallón (Multi-Hop Architecture)** para garantizar la calidad y trazabilidad del dato:

![Architecture Diagram](https://i.imgur.com/placeholder-diagram.png) *(Puedes subir tu propio diagrama aquí)*

### 1. 🥉 Capa Bronce (Raw Layer)
* **Objetivo:** Ingesta rápida de datos crudos "tal cual" llegan de la fuente.
* **Estrategia de Carga:** Full Load (Truncate & Insert).
* **Transformaciones:** Ninguna. Se mantiene la estructura original para auditoría.
* **Tablas:** `bronze.crm_cust_info`, `bronze.erp_loc_a101`, etc.

### 2. 🥈 Capa Plata (Silver Layer)
* **Objetivo:** Datos limpios, estandarizados y conformados.
* **Transformaciones:**
    * Manejo de nulos y duplicados.
    * Estandarización de nombres (ej. "M" -> "Male").
    * Validación de fechas y rangos.
    * Derivación de nuevas columnas.
* **Lógica:** Se aplica lógica técnica de limpieza, pero no reglas de negocio complejas de agregación.

### 3. 🥇 Capa Oro (Gold Layer)
* **Objetivo:** Datos listos para consumo de negocio y herramientas de BI (PowerBI, Tableau).
* **Modelo:** **Star Schema** (Esquema de Estrella).
* **Componentes:**
    * **Dimensiones:** `dim_customers`, `dim_products`.
    * **Hechos:** `fact_sales`.
* **Transformaciones:** Modelado dimensional, creación de claves subrogadas (Surrogate Keys), agregaciones y cruces finales.

---

## 🛠️ Tecnologías Utilizadas

* **Base de Datos:** Microsoft SQL Server.
* **Lenguaje:** T-SQL (Transact-SQL).
* **Herramienta GUI:** SQL Server Management Studio (SSMS).
* **Diseño de Diagramas:** draw.io.
* **Control de Versiones:** Git & GitHub.

---

## 📂 Estructura del Repositorio

```text
├── datasets/          # Archivos CSV fuente (ERP y CRM)
├── docs/              # Documentación (Diagramas ER, Data Dictionary)
├── scripts/           # Scripts SQL
│   ├── init_database.sql   # Creación de DB y Schemas
│   ├── bronze/             # DDL y Stored Procedures capa Bronce
│   ├── silver/             # DDL y Stored Procedures capa Plata
│   └── gold/               # Vistas y DDL capa Oro
└── tests/             # Scripts de validación y Data Quality Checks
````

-----

## 🚀 Cómo ejecutar este proyecto

1.  **Prerrequisitos:** Tener instalado SQL Server y SSMS.
2.  **Clonar el repo:**
    ```bash
    git clone [https://github.com/tu-usuario/sql-data-warehouse-project.git](https://github.com/tu-usuario/sql-data-warehouse-project.git)
    ```
3.  **Inicializar BD:** Ejecutar `scripts/init_database.sql` para crear la base de datos y los esquemas (`bronze`, `silver`, `gold`).
4.  **Cargar Datos (ETL):**
      * Ejecutar los scripts de la carpeta `scripts/bronze` para cargar los CSV.
      * Ejecutar los Stored Procedures de `scripts/silver` para limpiar y transformar.
      * Crear las vistas de `scripts/gold` para el modelo final.
5.  **Tests:** Ejecutar los queries de la carpeta `tests/` para verificar la consistencia de los datos.

-----

## 📚 Conceptos Aprendidos

  * **ETL vs ELT:** Extracción, carga y transformación masiva usando `BULK INSERT`.
  * **Data Quality:** Uso de técnicas como `ISNULL`, `CASE WHEN`, `TRIM`, y validaciones de integridad referencial.
  * **SCD (Slowly Changing Dimensions):** Entendimiento de cómo manejar cambios en los atributos de las dimensiones.
  * **Separation of Concerns (SoC):** Mantener la lógica de extracción separada de la lógica de negocio.
  * **Naming Conventions:** Importancia de una nomenclatura estándar (Snake Case) para mantenimiento a largo plazo.

-----

## 📢 Créditos

Este proyecto fue desarrollado siguiendo el tutorial "SQL Data Warehouse from Scratch" de **Data with Baraa**.

  * [Video Original](https://www.youtube.com/watch?v=9GVqKuTVANE)
  * [Canal de Baraa](https://www.youtube.com/@DataWithBaraa)

<!-- end list -->
