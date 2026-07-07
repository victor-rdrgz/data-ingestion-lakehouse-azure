import os
import json
from datetime import timedelta

from loguru import logger
from pyspark.sql import SparkSession, functions as F

from motor_ingesta import MotorIngesta
from motor_ingesta.agregaciones import aniade_hora_utc, aniade_intervalos_por_aeropuerto


class FlujoDiario:
    """
    Clase encargada de orquestar el flujo diario de procesamiento de datos de vuelos.
    Lee datos desde un archivo JSON, aplica transformaciones necesarias (hora UTC, unión con días anteriores,
    cálculo de vuelo siguiente), y guarda el resultado en una tabla particionada por fecha.
    """
    def __init__(self, config_file: str):
        """
        Inicializa el objeto FlujoDiario de procesamiento diario leyendo el archivo de configuración
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
        self.spark = crear_spark_session(self.config['EXECUTION_ENVIRONMENT'])


    def procesa_diario(self, data_file: str):
        """
        Ejecuta el procesamiento diario del archivo JSON proporcionado, aplicando transformaciones
        específicas y guardando el resultado en una tabla particionada por FlightDate.

        Parameters
        ----------
        data_file: str
            Ruta al archivo JSON del día a procesar (local o en almacenamiento Azure)
        """
        try:
            motor_ingesta = MotorIngesta(self.config)
            flights_df = motor_ingesta.ingesta_fichero(data_file)
            flights_with_utc = aniade_hora_utc(self.spark, flights_df)
            dia_actual = flights_df.first().FlightDate
            dia_previo = dia_actual - timedelta(days=1)
            try:
                flights_previo = self.spark.read.table(
                    self.config['output_table'])\
                    .where(F.col('FlightDate') == dia_previo)
                logger.info(f'Leída partición del día {dia_previo} con éxito')
            except Exception as e:
                logger.info(f'No se han podido leer datos del día {dia_previo}: {str(e)}')
                flights_previo = None

            if flights_previo:
                for col in flights_previo.columns:
                    if col not in flights_with_utc.columns:
                        flights_with_utc = flights_with_utc.withColumn(
                            col,
                            F.lit(None).cast(flights_previo.schema[col].dataType)
                        )
                df_unido = flights_previo.unionByName(flights_with_utc.select(flights_previo.columns))
                df_unido.write.mode('overwrite').saveAsTable('tabla_provisional')
                df_unido = self.spark.read.table('tabla_provisional')

            else:
                df_unido = flights_with_utc           # lo dejamos como está

            df_with_next_flight = aniade_intervalos_por_aeropuerto(df_unido)

            if not self.spark.catalog.tableExists(self.config['output_table']):
                df_with_next_flight\
                    .coalesce(self.config['output_partitions'])\
                    .write\
                    .mode('overwrite') \
                    .partitionBy('FlightDate')\
                    .saveAsTable(self.config['output_table'])
            else:
                df_with_next_flight\
                    .coalesce(self.config['output_partitions'])\
                    .write\
                    .mode('overwrite') \
                    .insertInto(self.config['output_table'])
            # Borrar la tabla provisional si la hubiéramos creado anteriormente
            self.spark.sql("DROP TABLE IF EXISTS tabla_provisional")

        except Exception as e:
            logger.error(f'No se pudo escribir la tabla del fichero {data_file}')
            raise e


if __name__ == '__main__':
    flujo = FlujoDiario('../config/config.json')
    if flujo.config['EXECUTION_ENVIRONMENT'] == 'DATABRICKS_CONNECT':
        input_file = flujo.config['flights_input_files']['dbr_connect']
    else:
        input_file = flujo.config['flights_input_files']['local']
    flujo.procesa_diario(input_file)
