import os
import json
from datetime import datetime
from zoneinfo import ZoneInfo

from pyspark.sql import DataFrame as DF, functions as f
from pyspark.sql.functions import col

class MotorIngestaBatch:
    """
    Completar docstring
    """
    def __init__(self, config_file: str, data_source: str):
        """
        Inicializa el objeto MotorIngestaBatch de procesamiento diario leyendo el archivo de configuración
        y creando una sesión de Spark según el entorno indicado.
        Se define la función crear_spark_session para configurar la SparkSession/DatabricksSession

        Parameters
        ----------
        config_file: str
            Ruta al archivo JSON con la configuración del flujo (parámetros, rutas, entorno, etc.)
        """

        def crear_spark_session(execution_env):
            if execution_env == 'DATABRICKS_CONNECT':
                from databricks.connect import DatabricksSession
                os.environ['DATABRICKS_CONFIG_PROFILE'] = 'DATAENGINEERING'
                spark =  DatabricksSession.builder.getOrCreate()
                spark.conf.set('fs.azure.account.key.mastervdrp001as.blob.core.windows.net',
                               self.config['token_cuenta_almacenamiento']
                               )
                return spark
            else:
                from pyspark.sql import SparkSession
                return (
                    SparkSession.builder
                    .master('local[*]')
                    .appName('FlujoDiario')
                    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
                    .enableHiveSupport()
                    .getOrCreate()
                )

        with open(config_file, 'r') as f:
            self.config = json.load(f)
        self.data_source = data_source
        self.spark = crear_spark_session(self.config['EXECUTION_ENVIRONMENT'])
        self.landing_path = self.config['data_type_information'][data_source]['landing_path']
        self.base_bronze_path = self.config['data_type_information'][data_source]['base_target_path']
        self.bronzeCheckPointLocation = f"{self.base_bronze_path}/_checkpoint"
        self.schemaLocation = f"{self.base_bronze_path}/_checkpoint/_schema"


    def ingesta_dataset(self):
        bronze_path = self.crear_ruta_bronze()

        df = (self.spark.readStream
              .format("cloudFiles")
              .option("cloudFiles.format", "json")
              .option("recursiveFileLookup", "true")
              .option("pathGlobFilter", "*.json")
              .load(self.landing_path)
              .withColumn("_ingested_at", f.current_timestamp())
              .withColumn("_ingested_file", f.input_file_name())
              .coalesce(1)
              )

        (df.writeStream
         .format("delta")
         .trigger(availableNow=True)
         .option("mergeSchema", "true")
         .option("checkpointLocation", self.bronzeCheckPointLocation)
         .option("cloudFiles.schemaLocation", self.schemaLocation)
         .start(bronze_path)
         )


    def crear_ruta_bronze(self):
        now = datetime.now(ZoneInfo("Europe/Madrid")).replace(tzinfo=None)

        year = now.strftime("%Y")
        month = now.strftime("%m")
        day = now.strftime("%d")

        if self.config['data_type_information'][self.data_source]['periodicidad'] == 'horaria':
            hour = now.strftime("%H")
            return os.path.join(self.base_bronze_path, year, month, day, hour)
        else:
            return os.path.join(self.base_bronze_path, year, month, day)


if __name__ == "__main__":
    is_databricks_job = "DATABRICKS_RUNTIME_VERSION" in os.environ
    is_notebook = "DATABRICKS_RUNTIME_VERSION" in os.environ and "DATABRICKS_JOB_ID" not in os.environ
    if is_databricks_job:
        path_config = dbutils.widgets.get("config_path")
        data_source = dbutils.widgets.get("data_source")
    elif is_notebook:
        path_config = '/Volumes/mastervdrp001dbrp/default/config/azure_config.json'
        data_source = 'sales_orders'
    else:
        path_config = '../config/config.json'
        data_source = 'sales_orders'
    motorBatch = MotorIngestaBatch(path_config, data_source)
    motorBatch.ingesta_dataset()