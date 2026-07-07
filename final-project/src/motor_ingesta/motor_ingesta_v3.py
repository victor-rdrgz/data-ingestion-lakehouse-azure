import json
import logging
from typing import Dict, List, Any, Optional

import pyspark.sql.functions as F
from pyspark.sql.types import StructType
from pyspark.sql import DataFrame
from confluent_kafka.schema_registry import SchemaRegistryClient

from motor_ingesta.utils import (
    validate_config,
    validate_kafka_config,
    get_datasource_config,
    get_kafka_config,
    get_schema_registry_conf,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LandingStreamReader:
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
        partition_columns: List[str]
    ):
        self.data_source = data_source
        self.dataset = dataset
        self.format = dataset_format
        if self.format != 'binary':
            self.schema_json = StructType.fromJson(schema_json)
        if self.format not in allowed_file_formats:
            logger.error(
                f"Formato '{self.format}' no permitido para el dataset '{self.data_source}/{self.dataset}'. "
                f"Formatos permitidos: {allowed_file_formats}")
            raise ValueError(
                f"Formato '{self.format}' no es válido. Usa uno de: {', '.join(allowed_file_formats)}"
            )
        self.landing_path = landing_path
        self.raw_path = raw_path
        self.bronze_path = bronze_path
        self.dataset_landing_path = self.landing_path
        self.dataset_bronze_schema_location = f'{self.bronze_path}_schema'
        self.partition_columns = partition_columns
        logger.info(
            f"[LandingStreamReader] Inicializado dataset='{dataset}' "
            f"source='{data_source}' format='{self.format}' "
            f"paths: landing='{self.landing_path}', raw='{self.raw_path}', bronze='{self.bronze_path}' "
            f"partitions={self.partition_columns} schema={'sí' if self.schema_json else 'no'}"
        )

    def __str__(self) -> str:
        return (
            f"[LandingStreamReader] dataset='{self.dataset}' "
            f"source='{self.data_source}' format='{self.format}' "
            f"paths: landing='{self.landing_path}', raw='{self.raw_path}', bronze='{self.bronze_path}' "
            f"partitions={self.partition_columns} schema={'sí' if self.schema_json else 'no'}"
        )

    @staticmethod
    def validate_config(
            config: Dict[str, Any],
            data_source: str,
            dataset: str
    ) -> None:
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

    @classmethod
    def from_config(
        cls,
        config_path: str,
        data_source: str,
        dataset: str
    ) -> "LandingStreamReader":
        try:
            logger.info(f"Leyendo configuración desde: {config_path}")
            with open(config_path, 'r') as f:
                config = json.load(f)
            logger.info(f"Configuración leída correctamente para {data_source}/{dataset}")
            LandingStreamReader.validate_config(config, data_source, dataset)
            return cls(
                data_source=data_source,
                dataset=dataset,
                landing_path=config['data_type_information'][data_source][dataset]['landing_path'],
                raw_path=config['data_type_information'][data_source][dataset]['raw_path'],
                bronze_path=config['data_type_information'][data_source][dataset]['bronze_path'],
                dataset_format=config['data_type_information'][data_source][dataset]['dataset_format'],
                schema_json=config['data_type_information'][data_source][dataset].get('schema_json'),
                allowed_file_formats=config['allowed_file_formats'],
                partition_columns=config['data_type_information'][data_source][dataset]['partition_columns'],
            )
        except Exception as e:
            logger.exception(f"Error leyendo la configuración en LandingStreamReader.from_config: {e}")
            raise

    def add_metadata_columns(self, df: DataFrame) -> DataFrame:
        try:
            data_cols = df.columns
            if "timestamp" not in data_cols:
                logger.error("Falta columna 'timestamp' en el DataFrame.")
                raise ValueError(
                    "Error: el DataFrame no contiene la columna necesaria para particionar: timestamp. "
                    "Revisa el esquema de entrada.")
            metadata_cols = ['_ingested_at', '_ingested_filename', 'year', 'month', 'day', 'hour']
            logger.info("Añadiendo columnas de metadatos al DataFrame.")
            df = (df.withColumn("_ingested_at", F.current_timestamp())
                  .withColumn("_ingested_filename", F.replace(
                        F.input_file_name(),
                        F.lit(self.landing_path),
                        F.lit(self.raw_path)))
                  .withColumn("year", F.year("timestamp"))
                  .withColumn("month", F.month("timestamp"))
                  .withColumn("day", F.dayofmonth("timestamp"))
                  .withColumn("hour", F.hour("timestamp"))
                  )
            return df.select(metadata_cols + data_cols)
        except Exception as e:
            logger.exception(f"Error añadiendo columnas de metadatos: {e}")
            raise

    def read_autoloader(self) -> DataFrame:
        try:
            logger.info(f"Iniciando lectura desde Auto Loader en formato {self.format}")
            dbutils.fs.mkdirs(self.dataset_bronze_schema_location)
            if self.format == "csv":
                return (spark.readStream
                        .format("cloudFiles")
                        .option("cloudFiles.format", self.format)
                        .option("header", "true")
                        .schema(self.schema_json)
                        .option("cloudFiles.schemaLocation", self.dataset_bronze_schema_location)
                        .option("rescuedDataColumn", "_rescued_data")
                        .load(self.dataset_landing_path)
                        )
            elif self.format == "binary":
                return (
                    spark.readStream
                    .format("cloudFiles")
                    .option("cloudFiles.format", "binaryFile")
                    .option("pathGlobFilter", "*.jpg")
                    .option("recursiveFileLookup", "true")
                    .option("cloudFiles.schemaLocation", self.dataset_bronze_schema_location)
                    .load(self.dataset_landing_path)
                )
            else:
                return (spark.readStream
                        .format("cloudFiles")
                        .option("cloudFiles.format", self.format)
                        .schema(self.schema_json)
                        .option("cloudFiles.schemaLocation", self.dataset_bronze_schema_location)
                        .load(self.dataset_landing_path)
                        )
        except Exception as e:
            logger.exception(f"Error leyendo desde Auto Loader: {e}")
            raise

    def read(self) -> DataFrame:
        try:
            logger.info(f"Ejecutando lectura para dataset '{self.dataset}' con formato '{self.format}'")
            df = self.read_autoloader()
            if df:
                logger.info("Añadiendo columnas de metadatos al DataFrame leído.")
                df = df.transform(self.add_metadata_columns)
            return df
        except Exception as e:
            logger.exception(f"Error en LandingStreamReader.read: {e}")
            raise


class KafkaStreamReader:
    def __init__(
        self,
        kafka_config: Dict[str, str],
        data_source: str,
        dataset: str,
        raw_path: str,
        bronze_path: str,
        dataset_format: str,
        partition_columns: List[str],
        topic: str,
        schema_registry_conf: Dict[str, Any]
    ):
        self.kafka_config = kafka_config
        self.data_source = data_source
        self.dataset = dataset
        self.raw_path = raw_path
        self.bronze_path = bronze_path
        self.format = dataset_format
        self.partition_columns = partition_columns
        self.topic = topic
        self.schema_registry_conf = schema_registry_conf
        logger.info(f"KafkaStreamReader inicializado para {data_source}/{dataset} leyendo de topic '{topic}'")

    def __str__(self) -> str:
        return (
            f"[KafkaStreamReader] dataset='{self.dataset}' "
            f"source='{self.data_source}' format='{self.format}' "
            f"topic='{self.topic}' partitions={self.partition_columns} "
            f"paths: raw='{self.raw_path}', bronze='{self.bronze_path}'"
        )

    @staticmethod
    def get_datasource_config(datasource_config_path: str) -> Dict[str, Any]:
        try:
            logger.info(f"Leyendo configuración del datasource desde {datasource_config_path}")
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
        except Exception as e:
            logger.exception(f"Error inesperado leyendo {datasource_config_path}: {e}")
            raise

    @staticmethod
    def get_kafka_config(kafka_config_path: str) -> Dict[str, str]:
        try:
            logger.info(f"Leyendo configuración de Kafka desde {kafka_config_path}")
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
            logger.exception(f"Error leyendo {kafka_config_path}: {e}")
            raise

    @staticmethod
    def get_schema_registry_conf(schema_registry_conf_path: str) -> Dict[str, Any]:
        try:
            logger.info(f"Leyendo configuración de Schema Registry desde {schema_registry_conf_path}")
            with open(schema_registry_conf_path) as sr:
                schema_registry_conf = json.load(sr)
            logger.info("Configuración de Schema Registry cargada correctamente.")
            return schema_registry_conf
        except FileNotFoundError:
            logger.error(f"No se encontró el archivo {schema_registry_conf_path}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"JSON inválido en {schema_registry_conf_path}: {e}")
            raise
        except Exception as e:
            logger.exception(f"Error inesperado leyendo {schema_registry_conf_path}: {e}")
            raise

    @staticmethod
    def validate_kafka_config(
            config: Dict[str, Any],
            data_source: str,
            dataset: str,
            kafka_config: Dict[str, Any],
            schema_registry_conf: Dict[str, Any]
    ) -> None:
        if 'data_type_information' not in config:
            raise KeyError("Falta la clave 'data_type_information' en el config.")
        if data_source not in config['data_type_information']:
            raise KeyError(f"Falta el data_source '{data_source}' en 'data_type_information'.")
        if dataset not in config['data_type_information'][data_source]:
            raise KeyError(f"Falta el dataset '{dataset}' en data_source '{data_source}'.")
        dataset_conf = config['data_type_information'][data_source][dataset]
        required_fields = [
            'raw_path',
            'bronze_path',
            'dataset_format',
            'partition_columns',
            'topic'
        ]
        for field in required_fields:
            if field not in dataset_conf:
                raise KeyError(f"Falta el campo '{field}' en el dataset '{dataset}' de la fuente '{data_source}'.")
        if not isinstance(dataset_conf['partition_columns'], list) or not dataset_conf['partition_columns']:
            raise TypeError(
                f"El campo 'partition_columns' en el dataset '{dataset}' de la fuente '{data_source}' debe ser una lista no vacía."
            )
        if 'allowed_file_formats' not in config:
            raise KeyError("Falta la clave 'allowed_file_formats' en el config.")
        if not isinstance(config['allowed_file_formats'], list) or not config['allowed_file_formats']:
            raise TypeError("El campo 'allowed_file_formats' debe ser una lista no vacía en el config.")
        if dataset_conf['dataset_format'] not in config['allowed_file_formats']:
            raise ValueError(
                f"El campo 'dataset_format'='{dataset_conf['dataset_format']}' del dataset '{dataset}' "
                f"no está entre los formatos permitidos: {config['allowed_file_formats']}."
            )
        kafka_required = ['bootstrap.servers', 'security.protocol', 'sasl.mechanisms', 'sasl.username', 'sasl.password']
        for field in kafka_required:
            if field not in kafka_config:
                raise KeyError(f"Falta el campo '{field}' en kafka_config (client.properties) requerido para Kafka.")
        if not isinstance(schema_registry_conf, dict) or not schema_registry_conf:
            raise TypeError("schema_registry_conf debe ser un diccionario no vacío.")

    @classmethod
    def from_config(
        cls,
        datasource_config_path: str,
        kafka_config_path: str,
        schema_registry_conf_path: str,
        data_source: str,
        dataset: str
    ) -> "KafkaStreamReader":
        try:
            logger.info(f"Creando instancia de KafkaStreamReader para {data_source}/{dataset}")
            datasource_config = cls.get_datasource_config(datasource_config_path)
            kafka_config = cls.get_kafka_config(kafka_config_path)
            schema_registry_conf = cls.get_schema_registry_conf(schema_registry_conf_path)
            return cls(
                kafka_config=kafka_config,
                data_source=data_source,
                dataset=dataset,
                raw_path=datasource_config['data_type_information'][data_source][dataset]['raw_path'],
                bronze_path=datasource_config['data_type_information'][data_source][dataset]['bronze_path'],
                dataset_format=datasource_config['data_type_information'][data_source][dataset]['dataset_format'],
                partition_columns=datasource_config['data_type_information'][data_source][dataset]['partition_columns'],
                topic=datasource_config['data_type_information'][data_source][dataset]['topic'],
                schema_registry_conf=schema_registry_conf
            )
        except Exception as e:
            logger.exception(f"Error creando instancia de KafkaStreamReader: {e}")
            raise

    def get_kafka_options(self) -> Dict[str, str]:
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
            logger.exception(f"Error generando opciones de conexión a Kafka: {e}")
            raise

    def add_metadata_columns(self, df: DataFrame) -> DataFrame:
        try:
            data_cols = df.columns
            if "timestamp" not in data_cols:
                logger.error("Falta columna 'timestamp' en el DataFrame.")
                raise ValueError(
                    "❌ Error: el DataFrame no contiene la columna necesaria para particionar: timestamp. "
                    "Revisa el esquema de entrada.")
            metadata_cols = ['_ingested_at', '_ingested_filename', 'year', 'month', 'day', 'hour']
            logger.info("Añadiendo columnas de metadatos y partición al DataFrame.")
            df = (df.withColumn("_ingested_at", F.current_timestamp())
                  .withColumn(
                        "message_id",
                        F.concat_ws("-",
                                    F.col("topic"),
                                    F.col("partition").cast("string"),
                                    F.col("offset").cast("string"))
                  )
                  .withColumn("year", F.year("timestamp"))
                  .withColumn("month", F.month("timestamp"))
                  .withColumn("day", F.dayofmonth("timestamp"))
                  .withColumn("hour", F.hour("timestamp"))
                  )
            all_cols = [col for col in metadata_cols if col in df.columns] + [col for col in data_cols if col not in metadata_cols]
            return df.select(metadata_cols + data_cols)
        except Exception as e:
            logger.exception(f"Error añadiendo columnas de metadatos en KafkaStreamReader: {e}")
            raise

    def parse_value(self, df: DataFrame) -> DataFrame:
        try:
            logger.info(f"Deserializando campo 'value' como {self.format}")
            if self.format == "avro":
                from pyspark.sql.avro.functions import from_avro
                value_subject = f"{self.topic}-value"
                schema_str = SchemaRegistryClient(self.schema_registry_conf).get_latest_version(
                    value_subject).schema.schema_str
                return df.withColumn("parsed_value", from_avro(F.expr("substring(_value,6,length(_value)-5)"), schema_str))
            elif self.format == "json":
                json_schema = self.schema_registry_conf.get("json_schema")
                if json_schema:
                    return df.withColumn("parsed_value",
                                         F.from_json(F.col("_value").cast("string"), StructType.fromJson(json_schema)))
                else:
                    return df.withColumn("parsed_value", F.col("_value").cast("string"))
            else:
                logger.warning(f"Formato {self.format} no requiere deserialización.")
                return df
        except Exception as e:
            logger.exception(f"Error deserializando value en parse_value: {e}")
            raise

    def read(self) -> DataFrame:
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
            df = self.parse_value(df)
            df = self.add_metadata_columns(df)
            logger.info("Lectura de Kafka completada.")
            return df
        except Exception as e:
            logger.exception(f"Error en KafkaStreamReader.read: {e}")
            raise


class BronzeStreamWriter:
    def __init__(
        self,
        data_source: str,
        dataset: str,
        landing_path: str,
        raw_path: str,
        bronze_path: str,
        dataset_format: str,
        schema_json: Dict[str, Any],
        partition_columns: List[str]
    ):
        self.data_source = data_source
        self.dataset = dataset
        self.format = dataset_format
        self.landing_path = landing_path
        self.raw_path = raw_path
        self.bronze_path = bronze_path
        self.dataset_checkpoint_location = f'{bronze_path}_checkpoint'
        self.table = f'hive_metastore.bronze.{self.data_source}_{self.dataset}'
        self.query_name = f"bronze-{data_source}-{dataset}"
        self.schema_json = StructType.fromJson(schema_json)
        self.partition_columns = partition_columns
        logger.info(f"BronzeStreamWriter inicializado para {data_source}/{dataset}")

    def __str__(self) -> str:
        return (
            f"[BronzeStreamWriter] dataset='{self.dataset}' "
            f"source='{self.data_source}' format='{self.format}' "
            f"table='{self.table}' checkpoint='{self.dataset_checkpoint_location}' "
            f"partitions={self.partition_columns} "
            f"paths: landing='{self.landing_path}', raw='{self.raw_path}', bronze='{self.bronze_path}'"
        )

    @classmethod
    def from_config(cls, config_path: str, data_source: str, dataset: str) -> "BronzeStreamWriter":
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
                dataset_format=config['data_type_information'][data_source][dataset]['dataset_format'],
                schema_json=config['data_type_information'][data_source][dataset]['schema_json'],
                partition_columns=config['data_type_information'][data_source][dataset]['partition_columns']
            )
        except Exception as e:
            logger.exception(f"Error leyendo la configuración en BronzeStreamWriter.from_config: {e}")
            raise

    def archive_raw_files(self, df: DataFrame) -> None:
        try:
            if "_ingested_filename" in df.columns:
                logger.info("Archivando archivos raw hacia landing...")
                files = [row["_ingested_filename"] for row in df.select("_ingested_filename").distinct().collect()]
                for file in files:
                    if file:
                        file_landing_path = file.replace(self.raw_path, self.landing_path)
                        logger.debug(f"Moviendo archivo: {file_landing_path} -> {file}")
                        dbutils.fs.mkdirs(file[0:file.rfind('/') + 1])
                        dbutils.fs.mv(file_landing_path, file)
                logger.info("Archivos archivados correctamente.")
        except Exception as e:
            logger.exception(f"Error archivando archivos raw: {e}")
            raise

    def write_data(self, df: DataFrame) -> None:
        try:
            logger.info("Iniciando escritura a capa bronze...")
            dbutils.fs.mkdirs(self.bronze_path)
            spark.sql('CREATE DATABASE IF NOT EXISTS hive_metastore.bronze')
            logger.info("Escribiendo datos con formato Delta...")
            logger.debug("Esquema del DataFrame:")
            df.printSchema()
            logger.debug(f"Columnas de partición: {self.partition_columns}")
            (df.write
             .format("delta")
             .option("checkpointLocation", self.dataset_checkpoint_location)
             .mode("append")
             .option("mergeSchema", "true")
             .option("path", self.bronze_path)
             .partitionBy(*self.partition_columns)
             .save()
             )
            logger.info(f"Datos escritos en {self.bronze_path}")
            spark.sql(f"CREATE TABLE IF NOT EXISTS {self.table} USING DELTA LOCATION '{self.bronze_path}' ")
            logger.info(f"Tabla Delta registrada en metastore: {self.table}")
        except Exception as e:
            logger.exception(f"Error escribiendo en bronze: {e}")
            raise

    def append_2_bronze(self, batch_df: DataFrame, batch_id: int) -> None:
        try:
            if batch_df.isEmpty():
                logger.warning(f"Batch {batch_id} vacío. No se escribe nada.")
                return
            logger.info(f"Procesando batch {batch_id}")
            batch_df.persist()
            logger.debug("Llamando a write_data...")
            self.write_data(batch_df)
            logger.debug("Llamando a archive_raw_files...")
            self.archive_raw_files(batch_df)
            logger.debug("Liberando memoria del batch...")
            batch_df.unpersist()
            logger.info(f"Batch {batch_id} procesado con éxito.")
        except Exception as e:
            logger.exception(f"Error procesando el batch {batch_id}: {e}")
            raise
