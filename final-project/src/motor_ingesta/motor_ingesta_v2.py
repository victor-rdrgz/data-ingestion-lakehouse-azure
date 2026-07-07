import json

import pyspark.sql.functions as F
from pyspark.sql.types import StructType

from confluent_kafka.schema_registry import SchemaRegistryClient


class LandingStreamReader:

    def __init__(self, data_source, dataset, landing_path, raw_path,
                 bronze_path, dataset_format, schema_json, partition_columns, allowed_file_formats):
        self.data_source = data_source
        self.dataset = dataset
        self.format = dataset_format
        self.landing_path = landing_path
        self.raw_path = raw_path
        self.bronze_path = bronze_path
        self.dataset_landing_path = self.landing_path
        self.dataset_bronze_schema_location = f'{self.bronze_path}_schema'
        self.schema_json = StructType.fromJson(schema_json)
        self.partition_columns = partition_columns
        self.allowed_file_formats = allowed_file_formats

    def __str__(self):
        return (f"LandingStreamReader(datasource='{self.data_source}',dataset='{self.dataset}')")

    @classmethod
    def from_config(cls, config_path, data_source, dataset):
        with open(config_path, 'r') as f:
            config = json.load(f)
        return cls(
            data_source = data_source,
            dataset = dataset,
            landing_path = config['data_type_information'][data_source][dataset]['landing_path'],
            raw_path = config['data_type_information'][data_source][dataset]['raw_path'],
            bronze_path = config['data_type_information'][data_source][dataset]['bronze_path'],
            dataset_format = config['data_type_information'][data_source][dataset]['dataset_format'],
            schema_json = config['data_type_information'][data_source][dataset]['schema_json'],
            partition_columns=config['data_type_information'][data_source][dataset]['partition_columns'],
            allowed_file_formats = config['allowed_file_formats']
        )

    def add_metadata_columns(self, df):
        data_cols = df.columns
        if "timestamp" not in data_cols:
            raise ValueError(
                "❌ Error: el DataFrame no contiene la columna necesaria para particionar: timestamp'. "
                "Revisa el esquema de entrada.")

        metadata_cols = ['_ingested_at', '_ingested_filename', 'year', 'month', 'day', 'hour']

        df = (df.withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_ingested_filename", F.replace(F.input_file_name(), F.lit(self.landing_path), F.lit(self.raw_path)))
              .withColumn("year", F.year("timestamp"))
              .withColumn("month", F.month("timestamp"))
              .withColumn("day", F.dayofmonth("timestamp"))
              .withColumn("hour", F.hour("timestamp"))
              )

        # reordernamos columnas
        return df.select(metadata_cols + data_cols)

    def read_autoloader(self):
        dbutils.fs.mkdirs(self.dataset_bronze_schema_location)
        if self.format == "csv":
            return (spark.readStream
                    .format("cloudFiles")
                    .option("cloudFiles.format", self.format)
                    .option("header", "true")
                    # .option("cloudFiles.schemaHints", self.schema_json)
                    .schema(self.schema_json)
                    .option("cloudFiles.schemaLocation", self.dataset_bronze_schema_location)
                    .option("rescuedDataColumn", "_rescued_data")
                    .load(self.dataset_landing_path)
                    )
        else:
            return (spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", self.format)
                # .option("cloudFiles.schemaHints", self.schema_json)
                .schema(self.schema_json)
                .option("cloudFiles.schemaLocation", self.dataset_bronze_schema_location)
                .load(self.dataset_landing_path)
            )
    def read(self):
        df = None

        if (self.format in self.allowed_file_formats):
            df = self.read_autoloader()
        else:
            raise Exception(f"Format {self.format} not supported")

        if df:
            df = df.transform(self.add_metadata_columns)
        return df


class KafkaStreamReader:
    def __init__(self, kafka_config, data_source, dataset, raw_path,
                 bronze_path, dataset_format, partition_columns, topic, schema_registry_conf):
        self.kafka_config = kafka_config
        self.data_source = data_source
        self.dataset = dataset
        self.raw_path = raw_path
        self.bronze_path = bronze_path
        self.format = dataset_format
        self.partition_columns = partition_columns
        self.topic = topic
        self.schema_registry_conf = schema_registry_conf

    def __str__(self):
        return (f"KafkaStreamReader(datasource='{self.data_source}',dataset='{self.dataset}')")

    @staticmethod
    def get_datasource_config(datasource_config_path):
        try:
            with open(datasource_config_path, 'r') as ds:
                datasource_config = json.load(ds)
            return datasource_config
        except FileNotFoundError:
            raise FileNotFoundError(
                f"No se encontró el archivo de configuración del datasource: {datasource_config_path}")
        except json.JSONDecodeError as e:
            raise ValueError(f"El archivo de configuración del datasource no es un JSON válido: {e}")

    @staticmethod
    def get_kafka_config(kafka_config_path):
        try:
            kafka_config = {}
            with open(kafka_config_path) as kf:
                for line in kf:
                    line = line.strip()
                    if len(line) != 0 and line[0] != "#":
                        parameter, value = line.strip().split('=', 1)
                        kafka_config[parameter] = value.strip()
            return kafka_config
        except FileNotFoundError:
            raise FileNotFoundError(f"No se encontró el archivo de configuración de Kafka: {kafka_config_path}")
        except Exception as e:
            raise ValueError(f"Error leyendo el archivo de configuración de Kafka: {e}")

    @staticmethod
    def get_schema_registry_conf(schema_registry_conf_path):
        try:
            with open(schema_registry_conf_path) as sr:
                schema_registry_conf = json.load(sr)
            return schema_registry_conf
        except FileNotFoundError:
            raise FileNotFoundError(
                f"No se encontró el archivo de configuración de Schema Registry: {schema_registry_conf_path}")
        except json.JSONDecodeError as e:
            raise ValueError(f"El archivo de configuración de Schema Registry no es un JSON válido: {e}")

    @classmethod
    def from_config(cls, datasource_config_path, kafka_config_path, schema_registry_conf_path, data_source, dataset):
        datasource_config = cls.get_datasource_config(datasource_config_path)
        kafka_config = cls.get_kafka_config(kafka_config_path)
        schema_registry_conf = cls.get_schema_registry_conf(schema_registry_conf_path)

        return cls(
            kafka_config = kafka_config,
            data_source = data_source,
            dataset = dataset,
            raw_path = datasource_config['data_type_information'][data_source][dataset]['raw_path'],
            bronze_path = datasource_config['data_type_information'][data_source][dataset]['bronze_path'],
            dataset_format = datasource_config['data_type_information'][data_source][dataset]['dataset_format'],
            partition_columns = datasource_config['data_type_information'][data_source][dataset]['partition_columns'],
            topic = datasource_config['data_type_information'][data_source][dataset]['topic'],
            schema_registry_conf = schema_registry_conf
        )

    def get_kafka_options(self):
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

    def add_metadata_columns(self, df):
        data_cols = df.columns
        if "timestamp" not in data_cols:
            raise ValueError(
                "❌ Error: el DataFrame no contiene la columna necesaria para particionar: timestamp'. "
                "Revisa el esquema de entrada.")

        metadata_cols = ['_ingested_at', '_ingested_filename', 'year', 'month', 'day', 'hour']

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
        all_cols = [col for col in metadata_cols if col in df.columns] + [col for col in data_cols if
                                                                          col not in metadata_cols]

        # reordernamos columnas
        return df.select(metadata_cols + data_cols)

    def parse_value(self, df):
        if self.format == "avro":
            from pyspark.sql.avro.functions import from_avro
            value_subject = f"{self.topic}-value"
            schema_str = SchemaRegistryClient(self.schema_registry_conf).get_latest_version(
                value_subject).schema.schema_str
            # Quita los 5 bytes de cabecera de Confluent
            return df.withColumn("parsed_value", from_avro(F.expr("substring(_value,6,length(_value)-5)"), schema_str))
        elif self.format == "json":
            json_schema = self.schema_registry_conf.get("json_schema")  # Si lo tienes en config
            if json_schema:
                return df.withColumn("parsed_value",
                                     F.from_json(F.col("_value").cast("string"), StructType.fromJson(json_schema)))
            else:
                return df.withColumn("parsed_value", F.col("_value").cast("string"))
        else:
            return df


    def read(self):
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
        return self.add_metadata_columns(df)


class BronzeStreamWriter:
    def __init__(self, data_source, dataset, landing_path, raw_path, bronze_path, dataset_format, schema_json, partition_columns):
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

    def __str__(self):
        return (f"BronzeStreamWriter(datasource='{self.data_source}',dataset='{self.dataset}')")

    @classmethod
    def from_config(cls, config_path, data_source, dataset):
        with open(config_path, 'r') as f:
            config = json.load(f)
        return cls(
            data_source=data_source,
            dataset=dataset,
            landing_path=config['data_type_information'][data_source][dataset]['landing_path'],
            raw_path=config['data_type_information'][data_source][dataset]['raw_path'],
            bronze_path=config['data_type_information'][data_source][dataset]['bronze_path'],
            dataset_format=config['data_type_information'][data_source][dataset]['dataset_format'],
            schema_json=config['data_type_information'][data_source][dataset]['schema_json'],
            partition_columns = config['data_type_information'][data_source][dataset]['partition_columns']
        )

    def archive_raw_files(self, df):
        if "_ingested_filename" in df.columns:
            files = [row["_ingested_filename"] for row in df.select("_ingested_filename").distinct().collect()]
            for file in files:
                if file:
                    file_landing_path = file.replace(self.raw_path, self.landing_path)
                    dbutils.fs.mkdirs(file[0:file.rfind('/') + 1])
                    dbutils.fs.mv(file_landing_path, file)

    def write_data(self, df):
        print("=== SCHEMA BEFORE WRITE ===")
        df.printSchema()
        print("=== PARTITION COLUMNS ===")
        print(*self.partition_columns)
        dbutils.fs.mkdirs(self.bronze_path)
        spark.sql('CREATE DATABASE IF NOT EXISTS hive_metastore.bronze')
        (df.write
         .format("delta")
         .option("checkpointLocation", self.dataset_checkpoint_location)
         .mode("append")
         .option("mergeSchema", "true")
         .option("path", self.bronze_path)
         .partitionBy(*self.partition_columns)
         .save()
         )
        spark.sql(f"CREATE TABLE IF NOT EXISTS {self.table} USING DELTA LOCATION '{self.bronze_path}' ")

    def append_2_bronze(self, batch_df, batch_id):
        if batch_df.isEmpty():
            display(f"⚠️ Batch {batch_id} vacío. No se escribe nada.")
            return
        print('Executing  persist')
        batch_df.persist()
        print('executing write_data')
        self.write_data(batch_df)
        print('executing archive_raw_files')
        self.archive_raw_files(batch_df)
        print('executing unpersist')
        batch_df.unpersist()