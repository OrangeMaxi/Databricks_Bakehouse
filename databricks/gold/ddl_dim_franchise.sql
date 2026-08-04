-- Databricks SQL
-- Script de Creación de Tabla DDL para dim_franchise
-- Proyecto: Databricks_Bakehouse
-- Capa: Gold
-- Tabla: dim_franchise
-- Descripción: Tabla dimensional que almacena información única de las franquicias.
-- Creado por: Agente Ingeniero de Datos
-- Fecha de Creación: 2024-07-30

CREATE TABLE IF NOT EXISTS dev_catalog.gold.dim_franchise (
    franchiseID BIGINT NOT NULL,
    franchise_name STRING NOT NULL,
    franchise_city STRING NOT NULL,
    franchise_country STRING NOT NULL
)
USING DELTA
COMMENT 'Tabla dimensional de franquicias con información única y depurada.'
LOCATION 'dbfs:/UnityCatalog/dev_catalog/gold/dim_franchise';

ALTER TABLE dev_catalog.gold.dim_franchise
ADD CONSTRAINT pk_franchiseID PRIMARY KEY (franchiseID);
