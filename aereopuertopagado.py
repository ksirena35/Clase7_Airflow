#!/usr/bin/env python3

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import HiveContext
sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)



##leo el archivo parquet ingestado  desde HDFS y lo cargo en un dataframe.

df1 = spark.read.option("header", "true").parquet("hdfs://172.17.0.2:9000/ingest/yellow_tripdata_2021-01.parquet")
df2 = spark.read.option("header", "true").parquet("hdfs://172.17.0.2:9000/ingest/yellow_tripdata_2021-02.parquet")


##crear un data frame uniendo los viajes del mes 01 y mes 02 del 2021.

df1y2 = df1.union(df2)
from pyspark.sql.functions import col, month
enero_febrero = df1y2.filter((month(col("tpep_pickup_datetime")) == 1) | (month(col("tpep_pickup_datetime")) == 2))


##Viajes que tuvieron como inicio o destino aeropuertos, que hayan pagado con dinero.
###Modifique el tipo de dato de decimal a entero

enero_febrero = enero_febrero.withColumn('RateCodeID', col('RateCodeID').cast('int'))
enero_febrero = enero_febrero.withColumn('Payment_type', col('Payment_type').cast('int'))

###Filtre los aereopuertos
aereopuertos = enero_febrero.filter( (enero_febrero.RateCodeID == 2) | (enero_febrero.RateCodeID == 3) | (enero_febrero.RateCodeID == 4))


###Filtre los  viajes a aereopuertos que pagaron con cash.

dff = aereopuertos.filter(enero_febrero.Payment_type == 2)


##Dataframe final para insertar en  la tabla airport_trips.

from pyspark.sql.functions import to_timestamp

dfinsert = dff.select(
    dff.tpep_pickup_datetime.cast("timestamp").alias("tpep_pickup_datetime"),
    dff.airport_fee.cast("double"),
    dff.Payment_type.cast("int"),
    dff.tolls_amount.cast("double"),
    dff.total_amount.cast("double")
)

##Insert de la tabla airport_trips en la base de datos tripdata  previamente creada en Hive.

dfinsert.write.mode("append").format("hive").saveAsTable("tripdata.airport_trips")

