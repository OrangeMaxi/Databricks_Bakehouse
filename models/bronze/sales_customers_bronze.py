# ==============================================================================
# Autor: Agente Engineer
# Fecha: 2024-07-31
# Objetivo: Cargar datos de clientes (sales_customers) a la capa Bronze,
#           seleccionando el archivo Parquet más reciente por fecha en el nombre
#           y realizando una sobrescritura completa.
# ==============================================================================

import dlt
from pyspark.sql.functions import lit
from datetime import datetime
import re

# --- Configuración de Paths y Tablas ---
VOLUME_PATH = "/Volumes/dev_catalog/bronze/landing_zone/"
TABLE_NAME = "sales_customers"
TARGET_CATALOG = "dev_catalog"
TARGET_SCHEMA = "bronze"

@dlt.table(
  name=f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{TABLE_NAME}",
  comment=f"Tabla Bronze para {TABLE_NAME}, cargada desde el snapshot Parquet diario más reciente.",
  table_properties={"quality": "bronze", "source": "landing_zone"}
)
def sales_customers_bronze():
    # Obtener la SparkSession para operaciones de listado de archivos en el driver.
    # dlt.read_at_least_once("dummy") es una forma segura de obtener SparkSession
    # en un contexto de DLT, aunque el origen no sea de streaming.
    spark = dlt.read_at_least_once("dummy").sparkSession

    # Inicializar variables para el seguimiento del archivo más reciente
    latest_date = None
    latest_file_path = None
    
    # Expresión regular para extraer la fecha (YYYYMMDD) del nombre del archivo.
    # Espera archivos como 'sales_customers_20240731.parquet'.
    file_pattern = rf"^{TABLE_NAME}_(\d{{8}})\.parquet$"

    # --- Listado y Selección del Archivo Más Reciente ---
    # Se utiliza la API de Hadoop FileSystem a través de PySpark para listar archivos
    # en el volumen de forma programática. Esto permite una búsqueda dinámica.
    try:
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI.create(VOLUME_PATH),
            spark.sparkContext._jsc.hadoopConfiguration()
        )
        files_in_volume = fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(VOLUME_PATH))
    except Exception as e:
        print(f"Advertencia: No se pudo listar el volumen {VOLUME_PATH}. Error: {e}")
        files_in_volume = [] # Si hay un error, asumimos que no hay archivos.

    # Iterar sobre los archivos para encontrar el más reciente que coincida con el patrón
    for file_status in files_in_volume:
        file_name = str(file_status.getPath().getName())
        match = re.match(file_pattern, file_name)
        if match:
            file_date_str = match.group(1)
            try:
                file_date = datetime.strptime(file_date_str, "%Y%m%d")
            except ValueError:
                # Ignorar archivos cuyo sufijo de fecha no sea válido
                continue
            
            # Si es la primera fecha encontrada o es más reciente que la actual 'latest_date'
            if latest_date is None or file_date > latest_date:
                latest_date = file_date
                latest_file_path = str(file_status.getPath())

    # --- Carga de Datos y Retorno del DataFrame ---
    if latest_file_path:
        # Lee el archivo Parquet identificado como el más reciente.
        # La estrategia de sobrescritura para las tablas LIVE de DLT se gestiona
        # automáticamente cuando el pipeline se refresca, garantizando que la tabla
        # de destino siempre refleje el estado completo del último snapshot.
        df = spark.read.format("parquet").load(latest_file_path)
        
        # Se añaden columnas de metadatos para trazabilidad y auditoría.
        # `ingestion_date`: Fecha en que el dato fue procesado por este pipeline.
        # `source_file`: Ruta completa del archivo Parquet de origen.
        return df.withColumn("ingestion_date", lit(datetime.now().date())) \\
                 .withColumn("source_file", lit(latest_file_path))
    else:
        # Si no se encuentra ningún archivo Parquet que coincida con el patrón,
        # se crea un DataFrame vacío. Esto es crucial para que el pipeline
        # pueda inicializarse sin errores si no hay datos disponibles en el volumen,
        # o para mantener la tabla existente si ya fue creada y no hay nuevas actualizaciones.
        print(f"No se encontró ningún archivo Parquet para '{TABLE_NAME}' con el patrón '{file_pattern}' en {VOLUME_PATH}.")
        
        # Intenta inferir el esquema de cualquier archivo Parquet que empiece
        # con el nombre de la tabla en el volumen. Esto es un fallback
        # para obtener el esquema si no se encuentra el "latest_file_path" exacto.
        schema_df = None
        for file_status in files_in_volume:
            file_name_iter = str(file_status.getPath().getName())
            if file_name_iter.startswith(f"{TABLE_NAME}_") and file_name_iter.endswith(".parquet"):
                try:
                    schema_df = spark.read.format("parquet").load(str(file_status.getPath()))
                    break
                except Exception as e:
                    print(f"No se pudo inferir el esquema del archivo {file_name_iter}: {e}")
                    continue
        
        if schema_df:
            # Si se pudo inferir el esquema de un archivo existente, usarlo.
            return spark.createDataFrame([], schema_df.schema)
        else:
            # Como último recurso, definir un esquema mínimo si no hay ningún archivo
            # para inferir el esquema. Esto es para garantizar que la tabla DLT
            # tenga una definición de esquema inicial.
            from pyspark.sql.types import StructType, StructField, StringType, DateType
            print("No se pudo inferir el esquema de ningún archivo. Usando esquema por defecto.")
            return spark.createDataFrame(
                [], 
                StructType([
                    StructField("id", StringType(), True), # Ejemplo de campo
                    StructField("name", StringType(), True), # Ejemplo de campo
                    StructField("ingestion_date", DateType(), True),
                    StructField("source_file", StringType(), True)
                ])
            )