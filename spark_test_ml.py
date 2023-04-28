from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col
import sys
# 1. Crear una aplicación de Spark y cargar el archivo de texto como un RDD.
spark = SparkSession.builder.appName("Analisis de ventas").getOrCreate()
rdd = spark.sparkContext.textFile(sys.argv[1])

# 2. Utilizar la función map para convertir cada línea del RDD en una tupla con tres elementos.
rdd = rdd.map(lambda x: tuple(x.split(",")))

# 3. Calcular el valor total de ventas por producto
ventas_por_producto = rdd.map(lambda x: (x[0], float(x[1]) * int(x[2]))).reduceByKey(lambda x, y: x + y)

# Convertir el RDD en un DataFrame de Spark
df = ventas_por_producto.toDF(["producto", "ventas"])

# Crear el vector de características
assembler = VectorAssembler(inputCols=["ventas"], outputCol="features")
data = assembler.transform(df)

# Entrenar el modelo de clustering k-means
kmeans = KMeans(featuresCol="features", predictionCol="cluster", k=3)
model = kmeans.fit(data)

# Obtener los resultados
results = model.transform(data).select(col("producto"), col("ventas"), col("cluster"))

# Mostrar los resultados
print("Resultados del clustering:")
results.show()

# df.show()