-- Databricks Delta Live Tables
-- TAREA: [ID_TAREA_JIRA] - Carga inicial de datos de franquicias en capa Bronze

-- DOCUMENTACION:
-- Esta tabla ingesta datos crudos de franquicias desde la landing zone.
-- La tabla `sales_franchises` se crea en el esquema `bronze` del `dev_catalog`.
-- No se aplican transformaciones en esta etapa, solo se añade una marca de tiempo de procesamiento.

CREATE OR REFRESH STREAMING TABLE dev_catalog.bronze.sales_franchises
COMMENT "Tabla de franquicias en capa Bronze, ingesta directa desde Landing Zone."
AS SELECT
  CAST(franchiseID AS BIGINT) AS franchiseID,
  name,
  city,
  district,
  zipcode,
  country,
  size,
  CAST(longitude AS DOUBLE) AS longitude,
  CAST(latitude AS DOUBLE) AS latitude,
  CAST(supplierID AS BIGINT) AS supplierID,
  current_timestamp() AS processing_timestamp
FROM
  read_files("/Volumes/dev_catalog/bronze/landing_zone/sales_franchises.parquet");