from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import pyspark.sql.functions as F


def asignarCupoPrestamo(df):
    # Asignar diferentes montos de préstamo según el salario
    df_con_cupo = df.withColumn("cupo_prestamo", F.when(F.col("salario") >= 1000, 10000)
                                                 .when(F.col("salario") >= 2000, 20000)
                                                 .when(F.col("salario") >= 5000, 50000)
                                                 .otherwise(100000))
    
    return df_con_cupo


if __name__ == "__main__":
    spark = SparkSession\
        .builder \
        .appName("ClientesPrestamos")\
        .master("local[3]")\
        .config("spark.sql.shuffle.partitions", 3)\
        .getOrCreate()
    
    tiposStreamingDF = (spark.readStream\
                        .format("kafka")\
                        .option("kafka.bootstrap.servers", "kafka1:9092, kafka2:9092, kafka3:9092")\
                        .option("subscribe", "clientes")\
                        .option("startingOffsets", "earliest")\
                        .load())
    
    esquema = StructType([
        StructField("nombre", StringType()),
        StructField("apellido", StringType()),
        StructField("salario", DoubleType()),
        StructField("pais", StringType())
    ])
    
    parsedDF = tiposStreamingDF\
        .select("value")\
        .withColumn("value", F.col("value").cast(StringType()))\
        .withColumn("input", F.from_json(F.col("value"), esquema))\
        .withColumn("nombre", F.col("input.nombre"))\
        .withColumn("apellido", F.col("input.apellido"))\
        .withColumn("salario", F.col("input.salario"))\
        .withColumn("pais", F.col("input.pais"))

    clientesConCupoDF = asignarCupoPrestamo(parsedDF)

    salida = clientesConCupoDF\
        .writeStream\
        .queryName("query")\
        .outputMode("append")\
        .format("console")\
        .start()

    salida.awaitTermination()


from time import sleep
for x in range(50):
    spark.sql("select * from query").show(1000, False)
    sleep(1)

    
print('llego hasta aqui')