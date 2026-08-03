-- Databricks Delta Live Tables
-- TAREA: [ID_TAREA_JIRA] - Carga inicial de datos de clientes en capa Bronze

-- DOCUMENTACION:
-- Esta tabla ingesta datos crudos de clientes desde la landing zone.
-- La tabla `sales_customer` se crea en el esquema `bronze` del `dev_catalog`.
-- No se aplican transformaciones en esta etapa, solo se añade una marca de tiempo de procesamiento.

CREATE OR REFRESH STREAMING TABLE dev_catalog.bronze.sales_customer
COMMENT "Tabla de clientes en capa Bronze, ingesta directa desde Landing Zone."
AS SELECT
  CAST(customerID AS BIGINT) AS customerID,
  first_name,
  last_name,
  email_address,
  phone_number,
  address,
  city,
  state,
  country,
  continent,
  current_timestamp() AS processing_timestamp
FROM
  read_files("/Volumes/dev_catalog/bronze/landing_zone");
