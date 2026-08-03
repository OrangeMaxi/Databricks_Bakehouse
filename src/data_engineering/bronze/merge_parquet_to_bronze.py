# Databricks notebook source
# MAGIC %md
# MAGIC # ETL: Carga y Fusión de Datos desde Parquet a Tablas Bronze
# MAGIC
# MAGIC ## Descripción
# MAGIC Este notebook de Databricks se encarga de cargar datos desde archivos Parquet ubicados en una zona de aterrizaje (`landing_zone`) en el volumen de `dev_catalog.bronze` y fusionarlos (MERGE/UPSERT) en las tablas Delta correspondientes en la misma capa Bronze. El objetivo es asegurar la unicidad y la consistencia de los datos, actualizando registros existentes e insertando nuevos.
# MAGIC
# MAGIC ## Metodología
# MAGIC 1.  **Lectura de Archivos Parquet:** Se leen los archivos Parquet desde la ruta especificada.
# MAGIC 2.  **Operación MERGE:** Por cada tabla destino, se realiza una operación `MERGE INTO` que:
# MAGIC     *   **Actualiza** los registros existentes cuando se encuentra una coincidencia en la clave única.
# MAGIC     *   **Inserta** nuevos registros cuando no se encuentra una coincidencia.
# MAGIC
# MAGIC ## Configuración
# MAGIC -   **Catálogo:** `dev_catalog`
# MAGIC -   **Esquema:** `bronze`
# MAGIC -   **Ruta de Archivos Parquet (Landing Zone):** `/Volumes/dev_catalog/bronze/landing_zone/`
# MAGIC -   **Tablas de Destino:**
# MAGIC     *   `sales_customers` (Clave de Fusión: `customerID`)
# MAGIC     *   `sales_franchises` (Clave de Fusión: `franchiseID`)
# MAGIC     *   `sales_suppliers` (Clave de Fusión: `supplierID`)

import pyspark.sql.functions as F
from delta.tables import DeltaTable

# Definición de variables de entorno (Dev)
CATALOG = "dev_catalog"
SCHEMA = "bronze"
LANDING_ZONE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/landing_zone/"

# --- sales_customers ---
table_name_customers = f"{CATALOG}.{SCHEMA}.sales_customers"
parquet_file_path_customers = f"{LANDING_ZONE_PATH}sales_customers.parquet"
merge_key_customers = "customerID"

print(f"Procesando tabla: {table_name_customers}")
print(f"Ruta de archivo Parquet: {parquet_file_path_customers}")

# Leer el archivo Parquet para sales_customers
df_customers = spark.read.format("parquet").load(parquet_file_path_customers)

# Obtener la tabla Delta de destino
deltaTable_customers = DeltaTable.forName(spark, table_name_customers)

# Realizar la operación MERGE
deltaTable_customers.alias("target") \
    .merge(
        df_customers.alias("source"),
        f"target.{merge_key_customers} = source.{merge_key_customers}"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

print(f"Operación MERGE completada para {table_name_customers}")


# --- sales_franchises ---
table_name_franchises = f"{CATALOG}.{SCHEMA}.sales_franchises"
parquet_file_path_franchises = f"{LANDING_ZONE_PATH}sales_franchises.parquet"
merge_key_franchises = "franchiseID"

print(f"\nProcesando tabla: {table_name_franchises}")
print(f"Ruta de archivo Parquet: {parquet_file_path_franchises}")

# Leer el archivo Parquet para sales_franchises
df_franchises = spark.read.format("parquet").load(parquet_file_path_franchises)

# Obtener la tabla Delta de destino
deltaTable_franchises = DeltaTable.forName(spark, table_name_franchises)

# Realizar la operación MERGE
deltaTable_franchises.alias("target") \
    .merge(
        df_franchises.alias("source"),
        f"target.{merge_key_franchises} = source.{merge_key_franchises}"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

print(f"Operación MERGE completada para {table_name_franchises}")


# --- sales_suppliers ---
table_name_suppliers = f"{CATALOG}.{SCHEMA}.sales_suppliers"
parquet_file_path_suppliers = f"{LANDING_ZONE_PATH}sales_suppliers.parquet"
merge_key_suppliers = "supplierID"

print(f"\nProcesando tabla: {table_name_suppliers}")
print(f"Ruta de archivo Parquet: {parquet_file_path_suppliers}")

# Leer el archivo Parquet para sales_suppliers
df_suppliers = spark.read.format("parquet").load(parquet_file_path_suppliers)

# Obtener la tabla Delta de destino
deltaTable_suppliers = DeltaTable.forName(spark, table_name_suppliers)

# Realizar la operación MERGE
deltaTable_suppliers.alias("target") \
    .merge(
        df_suppliers.alias("source"),
        f"target.{merge_key_suppliers} = source.{merge_key_suppliers}"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

print(f"Operación MERGE completada para {table_name_suppliers}")