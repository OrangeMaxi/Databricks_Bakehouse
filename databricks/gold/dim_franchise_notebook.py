# Databricks notebook

# MAGIC %md
# ETL para dim_franchise (Gold Layer)

-- Databricks SQL
-- Script para Creación y Carga de Tabla dim_franchise
-- Proyecto: Databricks_Bakehouse
-- Capa: Gold
-- Tabla: dim_franchise
-- Descripción: Tabla dimensional que almacena información única de las franquicias, creada y poblada en Databricks.
-- Creado por: Agente Ingeniero de Datos
-- Fecha de Creación: 2024-07-30

# MAGIC %sql
CREATE SCHEMA IF NOT EXISTS dev_catalog.gold;

CREATE TABLE IF NOT EXISTS dev_catalog.gold.dim_franchise (
    franchiseID BIGINT NOT NULL COMMENT 'Identificador único de la franquicia.',
    franchise_name STRING NOT NULL COMMENT 'Nombre de la franquicia.',
    franchise_city STRING NOT NULL COMMENT 'Ciudad donde se ubica la franquicia.',
    franchise_country STRING NOT NULL COMMENT 'País donde se ubica la franquicia.'
)
USING DELTA
COMMENT 'Tabla dimensional de franquicias con información única y depurada.';

ALTER TABLE dev_catalog.gold.dim_franchise
ADD CONSTRAINT pk_franchiseID PRIMARY KEY (franchiseID);

# MAGIC %sql
INSERT INTO dev_catalog.gold.dim_franchise (franchiseID, franchise_name, franchise_city, franchise_country)
SELECT
    CAST(franchiseID AS BIGINT) AS franchiseID,
    COALESCE(franchise_name, 'N/D') AS franchise_name,
    COALESCE(franchise_city, 'N/D') AS franchise_city,
    COALESCE(franchise_country, 'N/D') AS franchise_country
FROM dev_catalog.silver.silver_sales_enriched
QUALIFY ROW_NUMBER() OVER (PARTITION BY franchiseID ORDER BY franchiseID) = 1;
