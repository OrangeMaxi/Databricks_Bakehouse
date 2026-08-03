-- Databricks Delta Live Tables
-- TAREA: [ID_TAREA_JIRA] - Carga inicial de datos de proveedores en capa Bronze

-- DOCUMENTACION:
-- Esta tabla ingesta datos crudos de proveedores desde la landing zone.
-- La tabla `sales_suppliers` se crea en el esquema `bronze` del `dev_catalog`.
-- No se aplican transformaciones en esta etapa, solo se añade una marca de tiempo de procesamiento.

CREATE OR REFRESH STREAMING TABLE dev_catalog.bronze.sales_suppliers
COMMENT "Tabla de proveedores en capa Bronze, ingesta directa desde Landing Zone."
AS SELECT
  CAST(supplierID AS BIGINT) AS supplierID,
  name,
  ingredient,
  continent,
  city,
  district,
  size,
  CAST(longitude AS DOUBLE) AS longitude,
  CAST(latitude AS DOUBLE) AS latitude,
  approved,
  current_timestamp() AS processing_timestamp
FROM
  read_files("/Volumes/dev_catalog/bronze/landing_zone/sales_suppliers.parquet");