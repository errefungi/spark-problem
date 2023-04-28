from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
# Crear sesión de Spark
spark = SparkSession.builder.appName("Bike Rentals Prediction").getOrCreate()
# Definir el esquema de datos
schema = StructType([
 StructField("hour", IntegerType(), True),
 StructField("temperature", FloatType(), True),
 StructField("visitors", IntegerType(), True),
 StructField("rentals", IntegerType(), True)
])
# Cargar los datos de entrenamiento
data = spark.read.format("csv").option("header",
"true").schema(schema).load("../../mlspark_test/bike_rentals.csv")
# Convertir las características a un vector denso
assembler = VectorAssembler(inputCols=["hour", "temperature", "visitors"],
outputCol="features")
data = assembler.transform(data)
# Dividir los datos en conjunto de entrenamiento y conjunto de prueba
train_data, test_data = data.randomSplit([0.7, 0.3])
# Crear modelo de regresión lineal y entrenarlo con los datos de entrenamiento
lr = LinearRegression(featuresCol="features", labelCol="rentals")
model = lr.fit(train_data)
# Hacer predicciones en el conjunto de prueba
predictions = model.transform(test_data)
# Mostrar las predicciones
predictions.select("rentals", "prediction").show()
