-- Databricks SQL
-- Script de Carga de Datos DML para dim_franchise
-- Proyecto: Databricks_Bakehouse
-- Capa: Gold
-- Tabla: dim_franchise
-- Descripción: Inserta datos únicos y depurados de franquicias desde silver_sales_enriched.
-- Creado por: Agente Ingeniero de Datos
-- Fecha de Creación: 2024-07-30

INSERT INTO dev_catalog.gold.dim_franchise (franchiseID, franchise_name, franchise_city, franchise_country)
SELECT
    CAST(franchiseID AS BIGINT) AS franchiseID,
    COALESCE(franchise_name, 'N/D') AS franchise_name,
    COALESCE(franchise_city, 'N/D') AS franchise_city,
    COALESCE(franchise_country, 'N/D') AS franchise_country
FROM dev_catalog.silver.silver_sales_enriched
QUALIFY ROW_NUMBER() OVER (PARTITION BY franchiseID ORDER BY franchiseID) = 1;
