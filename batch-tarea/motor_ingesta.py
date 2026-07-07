import os
from pyspark.sql import DataFrame as DF, functions as F

class MotorIngesta:
    """
    Completar docstring
    """
    def __init__(self, config: dict):
        """
         Inicializa el objeto MotorIngesta y configura la sesión de Spark en función del entorno de ejecución.

         Parameters
         ----------
         config: dict
             Diccionario con la configuración necesaria para la ejecución del motor de ingesta.
             Se especifica en él el entorno de ejecución: "DATABRICKS_CONNECT" o "PYSPARK".
         """

        self.config = config
        if config["EXECUTION_ENVIRONMENT"] == "DATABRICKS_CONNECT":
            from databricks.connect import DatabricksSession
            os.environ["DATABRICKS_CONFIG_PROFILE"] = "DATAENGINEERING"
            self.spark = DatabricksSession.builder.getOrCreate()
        else:
            from pyspark.sql import SparkSession
            self.spark = SparkSession.builder.master("local[*]").getOrCreate()





    def ingesta_fichero(self, json_path: str) -> DF:
        """
        Lee un fichero JSON desde la ruta proporcionada, lo aplana si tiene estructuras
        anidadas y selecciona únicamente las columnas definidas en la configuración,
        aplicando los tipos y comentarios especificados.

        Parameters
        ----------
        json_path: str
            Ruta al fichero JSON que contiene los datos a procesar.

        Returns
        -------
        DF
            DataFrame de Spark con las columnas seleccionadas y tipadas según
            `self.config["data_columns"]`, listo para su posterior procesamiento.
        """
        flights_day_df = self.spark.read.json(json_path)
        aplanado_df = self.aplana_df(flights_day_df)
        lista_obj_column = [F.col(d["name"])
                                .cast(d["type"])
                                .alias(d["name"], metadata={"comment": d.get("comment", "")})
                            for d in self.config["data_columns"]]
        return aplanado_df.select(*lista_obj_column)


    @staticmethod
    def aplana_df(df: DF) -> DF:
        """
        Aplana un DataFrame de Spark que tenga columnas de tipo array y de tipo estructura.

        :param df: DataFrame de Spark que contiene columnas de tipo array o columnas de tipo estructura, incluyendo
                   cualquier nivel de anidamiento y también arrays de estructuras. Asumimos que los nombres de los
                   campos anidados son todos distintos entre sí, y no van a coincidir cuando sean aplanados.
        :return: DataFrame de Spark donde todas las columnas de tipo array han sido explotadas y las estructuras
                 han sido aplanadas recursivamente.
        """
        to_select = []
        schema = df.schema.jsonValue()
        fields = schema["fields"]
        recurse = False

        for f in fields:
            if f["type"].__class__.__name__ != "dict":
                to_select.append(f["name"])
            else:
                if f["type"]["type"] == "array":
                    to_select.append(F.explode(f["name"]).alias(f["name"]))
                    recurse = True
                elif f["type"]["type"] == "struct":
                    to_select.append(f"{f['name']}.*")
                    recurse = True

        new_df = df.select(*to_select)
        return MotorIngesta.aplana_df(new_df) if recurse else new_df
