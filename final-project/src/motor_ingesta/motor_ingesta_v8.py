import json
import logging
from typing import Dict, List, Any, Optional

from pyspark.sql.avro.functions import from_avro
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery
from pyspark.sql.types import StructType
from confluent_kafka.schema_registry import SchemaRegistryClient

# Configuración de logging para ejecución en cluster o job
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LandingKafkaStreamReader:
    """
    Clase para leer archivos binarios provenientes de kafka en streaming desde una ruta landing, usando Auto Loader
    de Databricks. Permite añadir columnas de metadatos y controlar los formatos de archivo permitidos.
    """
    def __init__(
        self,
        data_source: str,
        dataset: str,
        schema_registry_conf: Dict[str, Any],
        landing_path: str,
        raw_path: str,
        bronze_path: str,
        dataset_format: str,
        allowed_file_formats: List[str],
        processing_type: str,
        topic:str,
        value_format: str,
        schema_json: Optional[Dict[str, Any]],
    ):
        """
            Inicializa el lector de la zona landing para flujos en streaming.
            Args:
                data_source: Nombre de la fuente de datos.
                dataset: Nombre del dataset.
                schema_registry_conf: Configuración para acceder al Schema Registry.
                landing_path: Ruta del almacenamiento landing.
                raw_path: Ruta de almacenamiento raw.
                bronze_path: Ruta de almacenamiento bronze.
                dataset_format: Formato de los archivos (json, csv, parquet, etc.).
                allowed_file_formats: Lista de formatos de archivo permitidos para lectura.
                processing_type: Tipo de procesamiento. Solo se admite 'streaming'.
            Raises:
                ValueError: Si el tipo de procesamiento no es 'streaming'.
        """
        if processing_type != 'streaming':
            logger.error(f"El tipo de procesamiento '{processing_type}' no es válido. Debe ser streaming")
            raise ValueError(f"El tipo de procesamiento '{processing_type}' no es válido. Debe ser streaming")
        self.data_source = data_source
        self.dataset = dataset
        self.schema_registry_conf = schema_registry_conf
        self.format = dataset_format
        self.landing_path = landing_path
        self.raw_path = raw_path
        self.bronze_path = bronze_path
        self.allowed_file_formats = allowed_file_formats
        self.bronze_schema_location = f'{self.bronze_path}_schema'
        self.topic=topic
        self.value_format = value_format
        self.schema_json = schema_json

    def __str__(self) -> str:
        """Devuelve una representación legible de la instancia."""
        return (
            f"[LandingKafkaStreamReader] dataset='{self.dataset}' "
            f"source='{self.data_source}' format='{self.format}' "
            f"topic='{self.topic}' value_format='{self.value_format}' "
            f"schema_registry={'Sí' if self.schema_registry_conf.get('url') else 'No'} "
            f"paths: landing='{self.landing_path}', raw='{self.raw_path}', bronze='{self.bronze_path}'"
        )

    @staticmethod
    def validate_config(config: Dict[str, Any], data_source: str, dataset: str) -> None:
        """
        Valida la existencia de los parámetros obligatorios para un dataset de streaming desde landing.
        Args:
            config: Configuración completa cargada (dict).
            data_source: Nombre de la fuente de datos.
            dataset: Nombre del dataset.
        Raises:
            KeyError: Si falta cualquier parámetro obligatorio.
            TypeError: Si los tipos de datos no son válidos.
        """
        try:
            if 'data_type_information' not in config:
                raise KeyError("Falta la clave 'data_type_information' en el config.")
            if data_source not in config['data_type_information']:
                raise KeyError(f"Falta el data_source '{data_source}' en 'data_type_information'.")
            if dataset not in config['data_type_information'][data_source]:
                raise KeyError(f"Falta el dataset '{dataset}' en data_source '{data_source}'.")

            dataset_conf = config['data_type_information'][data_source][dataset]

            required_fields = [
                'landing_path',
                'raw_path',
                'bronze_path',
                'dataset_format',
                'partition_columns',
                'processing_type'
            ]
            for field in required_fields:
                if field not in dataset_conf:
                    raise KeyError(f"Falta el campo obligatorio '{field}' en el dataset '{dataset}'.")

            if dataset_conf['processing_type'] != 'streaming':
                raise ValueError(f"El dataset '{dataset}' debe tener processing_type='streaming'.")

            if 'allowed_file_formats' not in config:
                raise KeyError("Falta la clave 'allowed_file_formats' en el config.")

            if not isinstance(config['allowed_file_formats'], list) or not config['allowed_file_formats']:
                raise TypeError("El campo 'allowed_file_formats' debe ser una lista no vacía.")

        except Exception as e:
            logger.exception(f"Error en LandingKafkaStreamReader.validate_config: {e}")
            raise

    @classmethod
    def from_config(
        cls,
        config_path: str,
        data_source: str,
        dataset: str
    ) -> "LandingKafkaStreamReader":
        """
        Crea una instancia de LandingKafkaStreamReader a partir de un archivo de configuración JSON.

        Args:
            config_path: Ruta al archivo de configuración.
            data_source: Nombre de la fuente de datos.
            dataset: Nombre del dataset.
        Returns:
            Instancia de LandingKafkaStreamReader.
        Raises:
            Exception: Si ocurre algún error de lectura o validación.
        """
        try:
            logger.info(f"Leyendo configuración desde: {config_path}")
            with open(config_path, 'r') as f:
                config = json.load(f)
            logger.info(f"Configuración leída correctamente para {data_source}/{dataset}")

            cls.validate_config(config, data_source, dataset)

            dataset_conf = config['data_type_information'][data_source][dataset]

            return cls(
                data_source=data_source,
                dataset=dataset,
                schema_registry_conf=config['schema_registry_api_credentials'],
                landing_path=dataset_conf['landing_path'],
                raw_path=dataset_conf['raw_path'],
                bronze_path=dataset_conf['bronze_path'],
                dataset_format=dataset_conf['dataset_format'],
                allowed_file_formats=config['allowed_file_formats'],
                processing_type=dataset_conf['processing_type'],
                topic=dataset_conf['topic'],
                value_format=dataset_conf['value_format'],
                schema_json=dataset_conf.get('schema_json')
            )

        except Exception as e:
            logger.exception(f"Error en LandingKafkaStreamReader.from_config: {e}")
            raise

    def add_metadata_columns(self, df: DataFrame) -> DataFrame:
        """
        Añade columnas de metadatos y de particionado temporal al DataFrame.
        Args:
            df: DataFrame de entrada.
        Returns:
            DataFrame con columnas adicionales de metadatos.
        Raises:
            ValueError: Si no existe la columna 'timestamp' en el DataFrame.
        """
        try:
            data_cols = df.columns
            if "_timestamp" not in data_cols:
                logger.error("Falta columna 'timestamp' en el DataFrame.")
                raise ValueError(
                    "Error: el DataFrame no contiene la columna timestamp. "
                    "Revisa el esquema de entrada.")

            metadata_cols = ['_ingested_at', '_ingested_filename', 'year', 'month', 'day', 'hour']

            logger.info("Añadiendo columnas de metadatos al DataFrame.")
            df = (df.withColumn("_ingested_at", F.current_timestamp())
                  .withColumn("_ingested_filename", F.input_file_name())
                  .withColumn("year", F.year("_timestamp"))
                  .withColumn("month", F.month("_timestamp"))
                  .withColumn("day", F.dayofmonth("_timestamp"))
                  .withColumn("hour", F.hour("_timestamp"))
                  )
            return df.select(metadata_cols + data_cols)
        except Exception as e:
            logger.exception(f"Error en LandingKafkaStreamReader.add_metadata_columns: {e}")
            raise

    def parse_key(self, df: DataFrame) -> DataFrame:
        """
        Parsea el campo `key` del DataFrame según el formato configurado.
        Args:
            df (DataFrame): DataFrame leído desde Kafka, con columna `_key` en formato binario.
        Returns:
            DataFrame: DataFrame con la columna `key` ya parseada y tipada.
        """
        try:
            logger.info("Deserializando campo 'key'")

            key_format = self.schema_registry_conf.get("key_format", "string")

            if key_format == "int":
                return df.withColumn("key", F.col("_key").cast("int"))

            elif key_format == "string":
                return df.withColumn("key", F.col("_key").cast("string"))

            elif key_format == "json":
                json_schema = self.schema_registry_conf.get("key_schema")
                if not json_schema:
                    raise ValueError("Se esperaba un schema JSON para la clave, pero no se encontró 'key_schema'.")
                return df.withColumn(
                    "key",
                    F.from_json(F.col("_key").cast("string"), StructType.fromJson(json_schema))
                )

            else:
                logger.warning(f"Formato de clave '{key_format}' no reconocido. Se deja la columna `_key` sin parsear.")
                return df

        except Exception as e:
            logger.exception(f"Error en LandingKafkaStreamReader.parse_key: {e}")
            raise

    def parse_value(self, df: DataFrame) -> DataFrame:
        """
        Parsea el campo value del DataFrame según el formato configurado.
        Args:
            df: DataFrame leído desde Kafka.
        Returns:
            DataFrame con la columna 'value' añadida con los valores parseados.
        """

        try:
            logger.info(f"Deserializando campo 'value' como {self.value_format}")
            if self.value_format == "avro":
                value_subject = f"{self.topic}-value"
                schema_str = SchemaRegistryClient(self.schema_registry_conf).get_latest_version(
                    value_subject).schema.schema_str
                return (df
                        .withColumn("value", from_avro(F.expr("substring(_value,6,length(_value)-5)"), schema_str))
                        .drop("_value"))
            elif self.value_format == "json":
                if not self.schema_json:
                    raise ValueError("Falta 'value_schema' para deserializar JSON plano desde Kafka.")
                return (
                    df.withColumn(
                            "value",
                            F.from_json(
                                F.col("_value").cast("string"),
                                StructType.fromJson(self.schema_json))
                        )
                        .drop("_value")
                )
            else:
                # si alcanzamos esta línea, algo está fallando al parsear
                raise ValueError(
                    f"No se ha podido parasear el DataFram."
                    f"DatasetFormat '{self.value_format}' no permitido."
                    f"Datasource: '{self.data_source} Dataset:{self.dataset}'."
                    f"Formatos permitidos: {self.allowed_file_formats}."
                )
        except Exception as e:
            logger.exception(f"Error en LandingKafkaStreamReader.parse_value: {e}")
            raise ValueError(f"Formato de valor '{self.value_format}' no soportado para el dataset '{self.dataset}: {e}'")


    def parse_df(self, df: DataFrame) -> DataFrame:
        """
        Aplica el parseo del campo `_key` y `_value` sobre un DataFrame leído desde landing
        que contiene los datos crudos escritos previamente por Kafka.
        Args:
            df: DataFrame leído desde Parquet (desde landing), que contiene las columnas `_key` y `_value`.
        Returns:
            DataFrame con las columnas `key` y `value` deserializadas y tipadas.
        """
        try:
            logger.info("Iniciando parseo del DataFrame desde landing (campos '_key' y '_value')")
            df = self.parse_key(df)
            df = self.parse_value(df)
            logger.info("Parseo completo aplicado al DataFrame desde landing.")
            return df
        except Exception as e:
            logger.exception(f"Error en LandingKafkaStreamReader.parse_df: {e}")
            raise

    def read(self) -> DataFrame:
        """
        Ejecuta la lectura en streaming y añade metadatos.
        Returns:
            DataFrame de streaming con columnas de metadatos añadidas.
        Raises:
            Exception: Si el formato no es soportado.
        """
        try:
            logger.info(f"Ejecutando lectura para dataset '{self.dataset}' con formato '{self.format}'")
            df = self.from_streaming_read()
            if df:
                logger.info("Parseamos el DataFrame")
                df = self.parse_df(df)
                logger.info("Añadiendo columnas de metadatos al DataFrame leído.")
                df = df.transform(self.add_metadata_columns)
            else:
                raise

            return df
        except Exception as e:
            logger.exception(f"Error en LandingKafkaStreamReader.read: {e}")
            raise

    def from_streaming_read(self) -> DataFrame:
        """
        Lee archivos en formato Parquet desde la zona landing en streaming usando Auto Loader.
        Returns:
            DataFrame en streaming listo para procesamiento.
        """
        try:
            logger.info(f"Iniciando lectura en streaming desde landing (Parquet): {self.landing_path}")
            dbutils.fs.mkdirs(self.bronze_schema_location)

            return (
                spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "parquet")
                .option("cloudFiles.schemaLocation", self.bronze_schema_location)
                .load(self.landing_path)
            )
        except Exception as e:
            logger.exception(f"Error en LandingKafkaStreamReader.from_streaming_read: {e}")
            raise

class LandingBatchReader:
    """
    Clase para leer archivos microbatch en streaming desde una ruta landing, usando Auto Loader de Databricks.
    Permite añadir columnas de metadatos y controlar los formatos de archivo permitidos.
    """

    def __init__(
        self,
        data_source: str,
        dataset: str,
        landing_path: str,
        raw_path: str,
        bronze_path: str,
        dataset_format: str,
        schema_json: Optional[Dict[str, Any]],
        allowed_file_formats: List[str],
        processing_type: str
    ):
        """
        Inicializa el lector de la zona landing.
        Args:
            data_source: Nombre de la fuente de datos.
            dataset: Nombre del dataset.
            landing_path: Ruta del almacenamiento landing.
            raw_path: Ruta de almacenamiento raw.
            bronze_path: Ruta de almacenamiento bronze.
            dataset_format: Formato de los archivos (json, csv, parquet, etc.).
            schema_json: Esquema del dataset en formato dict.
            allowed_file_formats: Lista de extensiones de archivos permitidos.
        """
        self.data_source = data_source
        self.dataset = dataset
        self.format = dataset_format
        # si el formato es binary, no tiene esquema
        if schema_json:
            self.schema_json = StructType.fromJson(schema_json)
        self.landing_path = landing_path
        self.raw_path = raw_path
        self.bronze_path = bronze_path
        self.bronze_schema_location = f'{self.bronze_path}_schema'
        self.processing_type = processing_type
        logger.info(
            f"[LandingBatchReader] Inicializado dataset='{dataset}' "
            f"source='{data_source}' format='{self.format}' "
            f"paths: landing='{self.landing_path}', raw='{self.raw_path}', bronze='{self.bronze_path}' "
            f"schema={schema_json if schema_json else 'no'}"
        )

    def __str__(self) -> str:
        """Devuelve una representación legible de la instancia."""
        return (
            f"[LandingBatchReader] dataset='{self.dataset}' "
            f"source='{self.data_source}' format='{self.format}' "
            f"paths: landing='{self.landing_path}', raw='{self.raw_path}', bronze='{self.bronze_path}' "
            f"schema: {self.schema_json if self.schema_json else 'no'}"
        )

    @staticmethod
    def validate_config(
            config: Dict[str, Any],
            data_source: str,
            dataset: str
    ) -> None:
        """
        Valida la existencia de los parámetros obligatorios para un dataset en la configuración.
        Args:
            config: Configuración completa cargada (dict).
            data_source: Nombre de la fuente de datos.
            dataset: Nombre del dataset.
        Raises:
            KeyError: Si falta cualquier parámetro obligatorio.
        """
        try:
            if 'data_type_information' not in config:
                raise KeyError("Falta la clave 'data_type_information' en el config.")
            if data_source not in config['data_type_information']:
                raise KeyError(f"Falta el data_source '{data_source}' en 'data_type_information'.")
            if dataset not in config['data_type_information'][data_source]:
                raise KeyError(f"Falta el dataset '{dataset}' en data_source '{data_source}'.")

            dataset_conf = config['data_type_information'][data_source][dataset]
            required_fields = [
                'landing_path',
                'raw_path',
                'bronze_path',
                'dataset_format',
                'partition_columns'
            ]
            for field in required_fields:
                if field not in dataset_conf:
                    raise KeyError(f"Falta el campo '{field}' en el dataset '{dataset}' de la fuente '{data_source}'.")

            if 'allowed_file_formats' not in config:
                raise KeyError("Falta la clave 'allowed_file_formats' en el config.")

            if not isinstance(dataset_conf['partition_columns'], list) or not dataset_conf['partition_columns']:
                raise TypeError(
                    f"El campo 'partition_columns' en el dataset '{dataset}' de la fuente '{data_source}' debe ser una lista no vacía."
                )
        except Exception as e:
            logger.exception(f"Error en LandingBatchReader.validate_config: {e}")
            raise

    @classmethod
    def from_config(
        cls,
        config_path: str,
        data_source: str,
        dataset: str
    ) -> "LandingBatchReader":
        """
        Crea una instancia a partir de un archivo de configuración JSON.
        Args:
            config_path: Ruta al archivo de configuración.
            data_source: Nombre de la fuente de datos.
            dataset: Nombre del dataset.
        Returns:
            Instancia de LandingBatchReader.
        """
        try:
            logger.info(f"Leyendo configuración desde: {config_path}")
            with open(config_path, 'r') as f:
                config = json.load(f)
            logger.info(f"Configuración leída correctamente para {data_source}/{dataset}")
            LandingBatchReader.validate_config(config, data_source, dataset)
            return cls(
                data_source=data_source,
                dataset=dataset,
                landing_path=config['data_type_information'][data_source][dataset]['landing_path'],
                raw_path=config['data_type_information'][data_source][dataset]['raw_path'],
                bronze_path=config['data_type_information'][data_source][dataset]['bronze_path'],
                dataset_format=config['data_type_information'][data_source][dataset]['dataset_format'],
                schema_json=config['data_type_information'][data_source][dataset].get('schema_json'),
                allowed_file_formats=config['allowed_file_formats'],
                processing_type=config['data_type_information'][data_source][dataset]['processing_type'],
            )
        except Exception as e:
            logger.exception(f"Error en LandingBatchReader.from_config: {e}")
            raise

    def add_metadata_columns(self, df: DataFrame) -> DataFrame:
        """
        Añade columnas de metadatos y de particionado temporal al DataFrame.
        Args:
            df: DataFrame de entrada.
        Returns:
            DataFrame con columnas adicionales de metadatos.
        Raises:
            ValueError: Si no existe la columna 'timestamp' en el DataFrame.
        """
        try:
            data_cols = df.columns
            if "timestamp" not in data_cols:
                logger.error("Falta columna 'timestamp' en el DataFrame.")
                raise ValueError(
                    "Error: el DataFrame no contiene la columna timestamp. "
                    "Revisa el esquema de entrada.")

            metadata_cols = ['_ingested_at', '_ingested_filename', 'year', 'month', 'day', 'hour']

            logger.info("Añadiendo columnas de metadatos al DataFrame.")
            df = (df.withColumn("_ingested_at", F.current_timestamp())
                  .withColumn("_ingested_filename", F.input_file_name())
                  .withColumn("year", F.year("timestamp"))
                  .withColumn("month", F.month("timestamp"))
                  .withColumn("day", F.dayofmonth("timestamp"))
                  .withColumn("hour", F.hour("timestamp"))
                  )
            return df.select(metadata_cols + data_cols)
        except Exception as e:
            logger.exception(f"Error en LandingBatchReader.add_metadata_columns: {e}")
            raise


    def from_batch_read(self) -> DataFrame:
        """
        Lee datos en streaming desde la zona landing usando Auto Loader (cloudFiles).
        Returns:
            DataFrame en streaming listo para procesamiento.
        """
        try:
            logger.info(f"Iniciando lectura desde Auto Loader en formato {self.format}")
            dbutils.fs.mkdirs(self.bronze_schema_location)
            if self.format == "csv":
                return (spark.read
                        .format("cloudFiles")
                        .option("cloudFiles.format", self.format)
                        .option("header", "true")
                        .schema(self.schema_json)
                        .option("recursiveFileLookup", "true")
                        .option("cloudFiles.schemaLocation", self.bronze_schema_location)
                        .option("rescuedDataColumn", "_rescued_data")
                        .load(self.landing_path)
                        )
            elif self.format == "binary":
                return (
                    spark.read
                    .format("cloudFiles")
                    .option("cloudFiles.format", "binaryFile")
                    .option("pathGlobFilter", "*.jpg")
                    .option("recursiveFileLookup", "true")
                    .option("cloudFiles.schemaLocation", self.bronze_schema_location)
                    .load(self.landing_path)
                )
            else:
                return (spark.read
                        .format("cloudFiles")
                        .option("cloudFiles.format", self.format)
                        .schema(self.schema_json)
                        .option("cloudFiles.schemaLocation", self.bronze_schema_location)
                        .option("recursiveFileLookup", "true")
                        .load(self.landing_path)
                        )
        except Exception as e:
            logger.exception(f"Error en LandingBatchReader.read_autoloader: {e}")
            raise

    def read(self) -> DataFrame:
        """
        Ejecuta la lectura en streaming y añade metadatos.
        Returns:
            DataFrame de streaming con columnas de metadatos añadidas.
        Raises:
            Exception: Si el formato no es soportado.
        """
        try:
            logger.info(f"Ejecutando lectura para dataset '{self.dataset}' con formato '{self.format}'")
            df = self.from_batch_read()
            logger.info("Añadiendo columnas de metadatos al DataFrame leído.")
            return df.transform(self.add_metadata_columns)
        except Exception as e:
            logger.exception(f"Error en LandingBatchReader.read: {e}")
            raise

class KafkaStreamReader:
    """
    Clase para leer mensajes desde un topic de Kafka (Confluent) en streaming con Spark.
    Permite deserializar los mensajes, añadir metadatos y procesar valores según el formato.
    """

    def __init__(
        self,
        kafka_config: Dict[str, str],
        data_source: str,
        dataset: str,
        bronze_path: str,
        key_format: str,
        value_format: str,
        topic: str,
        schema_registry_conf: Dict[str, Any]
    ):
        """
        Inicializa la clase con la configuración de Kafka y del dataset.
        Args:
            kafka_config: Configuración de Kafka (client.properties).
            data_source: Nombre de la fuente de datos.
            dataset: Nombre del dataset.
            bronze_path: Ruta bronze.
            value_format: Formato de serialización del mensaje (avro, json...).
            topic: Nombre del topic de Kafka.
            schema_registry_conf: Configuración del schema registry.
        """
        self.kafka_config = kafka_config
        self.data_source = data_source
        self.dataset = dataset
        self.bronze_path = bronze_path
        self.key_format = key_format
        self.value_format = value_format
        self.topic = topic
        self.schema_registry_conf = schema_registry_conf
        logger.info(f"KafkaStreamReader inicializado para {data_source}/{dataset} leyendo de topic '{topic}'")

    def __str__(self) -> str:
        """Devuelve una representación legible de la instancia."""
        return (
            f"[KafkaStreamReader] dataset='{self.dataset}' "
            f"source='{self.data_source}' format='{self.value_format}' "
            f"topic='{self.topic}' "
            f"bronze path='{self.bronze_path}'"
        )

    @staticmethod
    def get_datasource_config(datasource_config_path: str) -> Dict[str, Any]:
        """
        Lee la configuración de datasource desde un archivo JSON.
        Args:
            datasource_config_path: Ruta del archivo.
        Returns:
            Diccionario con la configuración.
        Raises:
            FileNotFoundError: Si el archivo no existe.
            ValueError: Si el archivo no es un JSON válido.
        """
        logger.info(f"Leyendo configuración del datasource desde {datasource_config_path}")
        try:
            with open(datasource_config_path, 'r') as ds:
                datasource_config = json.load(ds)
            logger.info("Configuración de datasource cargada correctamente.")
            return datasource_config
        except FileNotFoundError:
            logger.error(f"No se encontró el archivo {datasource_config_path}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"JSON inválido en {datasource_config_path}: {e}")
            raise

    @staticmethod
    def get_kafka_config(kafka_config_path: str) -> Dict[str, str]:
        """
        Lee el archivo de configuración de Kafka (client.properties).
        Args:
            kafka_config_path: Ruta del archivo.
        Returns:
            Diccionario con la configuración.
        Raises:
            FileNotFoundError: Si el archivo no existe.
            ValueError: Si hay errores al leer el archivo.
        """
        logger.info(f"Leyendo configuración de Kafka desde {kafka_config_path}")
        try:
            kafka_config = {}
            with open(kafka_config_path) as kf:
                for line in kf:
                    line = line.strip()
                    if len(line) != 0 and line[0] != "#":
                        parameter, value = line.strip().split('=', 1)
                        kafka_config[parameter] = value.strip()
            logger.info("Configuración de Kafka cargada correctamente.")
            return kafka_config
        except FileNotFoundError:
            logger.error(f"No se encontró el archivo {kafka_config_path}")
            raise
        except Exception as e:
            logger.error(f"Error leyendo {kafka_config_path}: {e}")
            raise

    @staticmethod
    def validate_kafka_config(
            config: Dict[str, Any],
            data_source: str,
            dataset: str,
            kafka_config: Dict[str, Any]
    ) -> None:
        """
        Valida la existencia y tipo de los parámetros obligatorios para un dataset de Kafka.
        Args:
            config: Configuración completa (dict).
            data_source: Nombre de la fuente de datos.
            dataset: Nombre del dataset.
            kafka_config: Configuración de Kafka (client.properties) como dict.
        Raises:
            KeyError, TypeError, ValueError: Si falta cualquier parámetro obligatorio o hay tipos incorrectos.
        """
        # Validar presencia de la configuración general del dataset
        try:
            if 'data_type_information' not in config:
                raise KeyError("Falta la clave 'data_type_information' en el config.")
            if data_source not in config['data_type_information']:
                raise KeyError(f"Falta el data_source '{data_source}' en 'data_type_information'.")
            if dataset not in config['data_type_information'][data_source]:
                raise KeyError(f"Falta el dataset '{dataset}' en data_source '{data_source}'.")

            dataset_conf = config['data_type_information'][data_source][dataset]

            # Campos obligatorios en el dataset para Kafka
            required_fields = [
                'bronze_path',
                'value_format',
                'partition_columns',
                'topic'
            ]
            for field in required_fields:
                if field not in dataset_conf:
                    raise KeyError(f"Falta el campo '{field}' en el dataset '{dataset}' de la fuente '{data_source}'.")

            # Tipos y validaciones específicas
            if not isinstance(dataset_conf['partition_columns'], list) or not dataset_conf['partition_columns']:
                raise TypeError(
                    f"El campo 'partition_columns' en el dataset '{dataset}' de la fuente '{data_source}' debe ser una lista no vacía."
                )

            if 'allowed_file_formats' not in config:
                raise KeyError("Falta la clave 'allowed_file_formats' en el config.")
            if not isinstance(config['allowed_file_formats'], list) or not config['allowed_file_formats']:
                raise TypeError("El campo 'allowed_file_formats' debe ser una lista no vacía en el config.")

            # value_format permitido
            if dataset_conf['value_format'] not in config['allowed_file_formats']:
                raise ValueError(
                    f"El campo 'value_format'='{dataset_conf['value_format']}' del dataset '{dataset}' "
                    f"no está entre los formatos permitidos: {config['allowed_file_formats']}."
                )

            # Validación básica de kafka_config
            kafka_required = ['bootstrap.servers', 'security.protocol', 'sasl.mechanisms', 'sasl.username', 'sasl.password']
            for field in kafka_required:
                if field not in kafka_config:
                    raise KeyError(f"Falta el campo '{field}' en kafka_config (client.properties) requerido para Kafka.")

            # Validación básica de schema_registry_conf (al menos el endpoint o algún parámetro esencial)
            if (
                not "schema_registry_api_credentials" in config
                or not isinstance(config["schema_registry_api_credentials"], dict)
            ):
                raise TypeError("schema_registry_conf debe ser un diccionario no vacío.")
        except Exception as e:
            logger.exception(f"Error en KafkaStreamReader.validate_kafka_config: {e}")
            raise

    @classmethod
    def from_config(
        cls,
        datasource_config_path: str,
        kafka_config_path: str,
        data_source: str,
        dataset: str
    ) -> "KafkaStreamReader":
        """
        Crea una instancia a partir de varios archivos de configuración.
        Args:
            datasource_config_path: Ruta al config de datasource.
            kafka_config_path: Ruta al client.properties de Kafka.
            schema_registry_conf_path: Ruta al config de schema registry.
            data_source: Nombre de la fuente de datos.
            dataset: Nombre del dataset.
        Returns:
            Instancia de KafkaStreamReader.
        """
        try:
            logger.info(f"Creando instancia de KafkaStreamReader para {data_source}/{dataset}")

            datasource_config = cls.get_datasource_config(datasource_config_path)
            kafka_config = cls.get_kafka_config(kafka_config_path)

            # comprobamos que la configuración venga correctamente antes de empezar
            cls.validate_kafka_config(datasource_config, data_source, dataset, kafka_config)

            return cls(
                kafka_config=kafka_config,
                data_source=data_source,
                dataset=dataset,
                bronze_path=datasource_config['data_type_information'][data_source][dataset]['bronze_path'],
                key_format=datasource_config['data_type_information'][data_source][dataset]['key_format'],
                value_format=datasource_config['data_type_information'][data_source][dataset]['value_format'],
                topic=datasource_config['data_type_information'][data_source][dataset]['topic'],
                schema_registry_conf=(
                    datasource_config['schema_registry_api_credentials'])
            )
        except Exception as e:
            logger.exception(f"Error en KafkaStreamReader.from_config: {e}")
            raise

    def get_kafka_options(self) -> Dict[str, str]:
        """
        Prepara las opciones para la lectura de Spark Structured Streaming desde Kafka.
        Returns:
            Diccionario de opciones para spark.readStream.
        """
        try:
            logger.info(f"Generando opciones de conexión a Kafka para el topic '{self.topic}'")
            return {
                "kafka.bootstrap.servers": self.kafka_config["bootstrap.servers"],
                "kafka.security.protocol": self.kafka_config["security.protocol"],
                "kafka.sasl.mechanism": self.kafka_config["sasl.mechanisms"],
                "kafka.sasl.jaas.config": f"""
                    kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required 
                    username="{self.kafka_config['sasl.username']}" 
                    password="{self.kafka_config['sasl.password']}";
                """,
                "subscribe": self.topic,
                "includeHeaders": "true",
                "startingOffsets": "earliest"
            }
        except Exception as e:
            logger.exception(f"Error en KafkaStreamReader.get_kafka_options: {e}")
            raise

    def read(self) -> DataFrame:
        """
        Lee mensajes desde Kafka en streaming
        Returns:
            DataFrame resultante.
        """
        try:
            logger.info(f"Iniciando lectura del topic Kafka: {self.topic}")
            kafka_options = self.get_kafka_options()

            df = (spark
                  .readStream
                  .format("kafka")
                  .options(**kafka_options)
                  .load()
                  )
            columns = [F.col(column).alias(f'_{column}') for column in df.columns]
            df = df.select(*columns)
            logger.info("Lectura de Kafka completada.")
            return df.withColumn("_ingested_to_landing_at", F.current_timestamp())
        except Exception as e:
            logger.exception(f"Error en KafkaStreamReader.read: {e}")
            raise

class LandingStreamWriter:
    """
    Clase encargada de escribir un streaming DataFrame en la capa landing en formato Parquet.
    """
    def __init__(
        self,
        data_source: str,
        dataset: str,
        landing_path: str
    ):
        self.data_source = data_source
        self.dataset = dataset
        self.landing_path = landing_path
        self.checkpoint_location = f'{landing_path}_checkpoint'
        self.query_name = f"landing-{data_source}-{dataset}"

    def __str__(self) -> str:
        return (
            f"[LandingStreamWriter] dataset='{self.dataset}' "
            f"source='{self.data_source}' "
            f"landing path='{self.landing_path}' "
            f"checkpoint path='{self.checkpoint_location}' "
        )

    @classmethod
    def from_config(cls, config_path: str, data_source: str, dataset: str) -> "LandingStreamWriter":
        """
        Crea una instancia de LandingStreamWriter a partir de un archivo JSON.
        Args:
            config_path: Ruta del archivo de configuración.
            data_source: Nombre de la fuente.
            dataset: Nombre del dataset.
        Returns:
            Instancia de LandingStreamWriter.
        """
        try:
            logger.info(f"Leyendo configuración desde: {config_path}")
            with open(config_path, 'r') as f:
                config = json.load(f)
            logger.info(f"Configuración cargada para {data_source}/{dataset}")
            return cls(
                data_source=data_source,
                dataset=dataset,
                landing_path=config['data_type_information'][data_source][dataset]['landing_path']
            )
        except Exception as e:
            logger.exception(f"Error en LandingStreamWriter.from_config: {e}")
            raise

    def write_stream(self, df: DataFrame) -> StreamingQuery:
        """
        Inicia la escritura en streaming del DataFrame hacia la capa landing (formato Parquet).
        Args:
            df: DataFrame de entrada en streaming.
        Returns:
            StreamingQuery: Objeto que representa la consulta de streaming iniciada.
        """
        try:
            logger.info("Iniciando escritura en landing")
            dbutils.fs.mkdirs(self.landing_path)
            logger.info("Escribiendo datos con formato Parquet")

            stream_query = (df.writeStream
                                 .format("parquet")
                                 .option("checkpointLocation", self.checkpoint_location)
                                 .queryName(self.query_name)
                                 .outputMode("append")
                                 .option("mergeSchema", "true")
                                 .option("path", self.landing_path)
                                 .start()
                            )
            logger.info(f"StreamingQuery iniciado para query '{self.query_name}'")
            return stream_query
        except Exception as e:
            logger.exception(f"Error en LandingStreamWriter.write_stream_to_landing: {e}")
            raise

class BronzeWriter:
    """
    Clase unificada encargada de escribir un DataFrame en la capa bronze,
    ya sea en modo batch o streaming, aplicando particionamiento, checkpoint
    y persistencia en Delta Lake.
    """

    def __init__(
        self,
        data_source: str,
        dataset: str,
        landing_path: str,
        raw_path: str,
        bronze_path: str,
        partition_columns: list,
        processing_type: str
    ):
        """
        Inicializa el escritor de la capa bronze.
        Args:
            data_source (str): Nombre de la fuente de datos.
            dataset (str): Nombre del dataset.
            landing_path (str): Ruta de almacenamiento landing.
            raw_path (str): Ruta de almacenamiento raw.
            bronze_path (str): Ruta de almacenamiento bronze.
            partition_columns (list): Columnas para particionar el almacenamiento.
            processing_type (str): Tipo de procesamiento, puede ser 'batch' o 'streaming'.
        """
        self.data_source = data_source
        self.dataset = dataset
        self.landing_path = landing_path
        self.raw_path = raw_path
        self.bronze_path = bronze_path
        self.checkpoint_location = f"{bronze_path}_checkpoint"
        self.table = f"hive_metastore.bronze.{self.data_source}_{self.dataset}"
        self.query_name = f"bronze-{data_source}-{dataset}"
        self.partition_columns = partition_columns
        self.processing_type = processing_type
        logger.info(f"BronzeWriter inicializado para {data_source}/{dataset} en modo {processing_type}")

    def __str__(self) -> str:
        """
        Devuelve una representación legible de la instancia.
        """
        return (
            f"[BronzeWriter] dataset='{self.dataset}' source='{self.data_source}' "
            f"table='{self.table}' checkpoint='{self.checkpoint_location}' "
            f"partitions={self.partition_columns} "
            f"paths: landing='{self.landing_path}', raw='{self.raw_path}', bronze='{self.bronze_path}' "
            f"mode={self.processing_type}"
        )

    @classmethod
    def from_config(cls, config_path: str, data_source: str, dataset: str) -> "BronzeWriter":
        """
        Crea una instancia de BronzeWriter a partir de un archivo JSON de configuración.
        Args:
            config_path (str): Ruta al archivo de configuración.
            data_source (str): Nombre de la fuente de datos.
            dataset (str): Nombre del dataset.
        Returns:
            BronzeWriter: Instancia configurada.
        """
        try:
            logger.info(f"Leyendo configuración desde: {config_path}")
            with open(config_path, 'r') as f:
                config = json.load(f)
            logger.info(f"Configuración cargada para {data_source}/{dataset}")
            return cls(
                data_source=data_source,
                dataset=dataset,
                landing_path=config['data_type_information'][data_source][dataset]['landing_path'],
                raw_path=config['data_type_information'][data_source][dataset]['raw_path'],
                bronze_path=config['data_type_information'][data_source][dataset]['bronze_path'],
                partition_columns=config['data_type_information'][data_source][dataset]['partition_columns'],
                processing_type=config['data_type_information'][data_source][dataset]['processing_type']
            )
        except Exception as e:
            logger.exception(f"Error en BronzeWriter.from_config: {e}")
            raise

    def archive_raw_files(self, df: DataFrame) -> None:
        """
        Mueve los archivos desde landing hacia la zona raw si existe la columna '_ingested_filename'.
        Args:
            df (DataFrame): DataFrame del batch o microbatch a procesar.
        """
        try:
            if "_ingested_filename" in df.columns:
                logger.info("Archivando archivos desde landing hacia raw...")
                df = df.withColumn(
                    "_ingested_filename",
                    F.regexp_replace(F.col("_ingested_filename"), self.landing_path, self.raw_path)
                )
                files = [row["_ingested_filename"] for row in df.select("_ingested_filename").distinct().collect()]
                for file in files:
                    if file:
                        file_landing_path = file.replace(self.raw_path, self.landing_path)
                        logger.debug(f"Moviendo archivo: {file_landing_path} -> {file}")
                        dbutils.fs.mkdirs(file[0:file.rfind('/') + 1])
                        dbutils.fs.mv(file_landing_path, file)
                logger.info("Archivos archivados correctamente.")
        except Exception as e:
            logger.exception(f"Error en BronzeWriter.archive_raw_files: {e}")
            raise

    def write_to_bronze(self, df: DataFrame) -> None:
        """
        Realiza la escritura del DataFrame en formato Delta en la capa bronze,
        particionando según columnas configuradas. Soporta batch o streaming según configuración.
        Args:
            df (DataFrame): DataFrame a escribir.
        """
        try:
            logger.info("Escribiendo a bronze...")
            dbutils.fs.mkdirs(self.bronze_path)
            spark.sql('CREATE DATABASE IF NOT EXISTS hive_metastore.bronze')
            writer = (df.writeStream if self.processing_type == 'streaming' else df.write)

            if self.processing_type == 'streaming':
                writer = (df.writeStream
                          .format("delta")
                          .option("checkpointLocation", self.checkpoint_location)
                          .outputMode("append")
                          .option("mergeSchema", "true")
                          .option("path", self.bronze_path)
                          .partitionBy(*self.partition_columns)
                          .queryName(self.query_name)
                          .start())
            else:
                (df.write
                 .format("delta")
                 .mode("append")
                 .option("mergeSchema", "true")
                 .partitionBy(*self.partition_columns)
                 .save(self.bronze_path))

            spark.sql(f"CREATE TABLE IF NOT EXISTS {self.table} USING DELTA LOCATION '{self.bronze_path}'")
            logger.info(f"Escritura completada y tabla registrada: {self.table}")
        except Exception as e:
            logger.exception(f"Error en BronzeWriter.write_to_bronze: {e}")
            raise

    def append_2_bronze(self, df: DataFrame, df_id: Optional[int] = None) -> None:
        """
        Aplica el flujo completo de escritura: escritura en bronze, archivado y limpieza.
        Args:
            df (DataFrame): DataFrame a procesar.
            df_id (Optional[int]): ID del batch o microbatch, útil para logs.
        """
        try:
            if df_id is not None:
                logger.info(f"Procesando batch {df_id}")
            df.persist()

            logger.debug("Ejecutando escritura en bronze")
            self.write_to_bronze(df)

            logger.debug("Archivando archivos crudos")
            self.archive_raw_files(df)

            df.unpersist()
            logger.info(f"Batch {df_id} procesado correctamente")
        except Exception as e:
            logger.exception(f"Error en BronzeWriter.append: {e}")
            raise
