import os
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession, DataFrame as DF, functions as F, Window


def aniade_hora_utc(spark: SparkSession, df: DF) -> DF:
    """
    Añade una columna 'FlightTime' en formato UTC a partir de la fecha y hora local de salida
    de los vuelos. Para calcular el UTC, se utiliza un fichero CSV con las zonas horarias de
    los aeropuertos de origen.

    Parameters
    ----------
    spark: SparkSession
        Sesión de Spark activa.
    df: DF
        DataFrame original que contiene los datos de los vuelos, con columnas 'FlightDate',
        'DepTime' y 'Origin'.
    Returns
    -------
    DF
        DataFrame original con la columna adicional 'FlightTime' en formato UTC y
        sin las columnas auxiliares.
    """

    # Carga el CSV de zonas horarias
    path_timezones = Path(__file__).parent / "resources" / "timezones.csv"
    timezones_pd = pd.read_csv(path_timezones)
    timezones_df = spark.createDataFrame(timezones_pd)

    df_with_tz = df.join(
        timezones_df,
        df['Origin'] == timezones_df['iata_code'],
        how='left'
    )
    # Añade la columna castedHour (DepTime como string de 4 dígitos con ceros a la izquierda)
    df_with_casted = df_with_tz.withColumn(
        'castedHour',
        F.lpad(F.col('DepTime').cast('string'), 4, '0')
    )

    # Crea la columna FlightTime local como timestamp
    df_with_flight_time = df_with_casted.withColumn(
        'FlightTime',
        F.concat(
            F.col('FlightDate').cast('string'),
            F.lit(' '),
            F.col('castedHour').substr(1, 2),
            F.lit(':'),
            F.col('castedHour').substr(3, 2),
            F.lit(':00')
        ).cast('timestamp')
    )

    # Convierte FlightTime local a UTC usando zona horaria
    df_utc = df_with_flight_time.withColumn(
        'FlightTime',
        F.to_utc_timestamp(F.col('FlightTime'), F.col('iana_tz'))
    )

    # Elimina las columnas auxiliares (las del timezones_df + castedHour)
    columnas_a_borrar = list(timezones_df.columns) + ['castedHour']
    df_final = df_utc.drop(*columnas_a_borrar)
    return df_final


def aniade_intervalos_por_aeropuerto(df: DF) -> DF:
    """
    Añade información del siguiente vuelo desde el mismo aeropuerto de origen.
    Crea tres columnas:
      - FlightTime_next: timestamp del siguiente vuelo (desde el mismo aeropuerto)
      - Airline_next: aerolínea del siguiente vuelo
      - diff_next: diferencia en segundos entre FlightTime actual y el siguiente

    Parameters
    ----------
    df: DF
        DataFrame con al menos las columnas 'Origin', 'FlightTime' y 'Reporting_Airline'.

    Returns
    -------
    DF
        DataFrame con columnas adicionales de información sobre el siguiente vuelo.
    """
    # Definimos una ventana por aeropuerto de origen ordenada por la hora del vuelo
    w = Window.partitionBy('Origin').orderBy('FlightTime')

    # Creamos una columna auxiliar que combina FlightTime y Reporting_Airline
    df = df.withColumn('pair', F.struct('FlightTime', 'Reporting_Airline'))

    # Usamos lag negativo para obtener el siguiente vuelo dentro de la misma ventana
    df = df.withColumn('pair_next', F.lag('pair', -1).over(w))

    # Extraemos las columnas FlightTime_next y Airline_next del struct
    df = df.withColumn('FlightTime_next', F.col('pair_next.FlightTime'))
    df = df.withColumn('Airline_next', F.col('pair_next.Reporting_Airline'))

    # Calculamos la diferencia en segundos
    df = df.withColumn(
        'diff_next',
        (F.col('FlightTime_next').cast('long') - F.col('FlightTime').cast('long'))
    )

    # Eliminamos las columnas auxiliares
    df_with_next_flight = df.drop('pair', 'pair_next')

    return df_with_next_flight
