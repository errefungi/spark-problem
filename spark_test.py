from pyspark.sql import SparkSession

# 1. Crear una aplicación de Spark y cargar el archivo de texto como un RDD.
spark = SparkSession.builder.appName("Analisis de ventas").getOrCreate()
rdd = spark.sparkContext.textFile("../../test_problem/test_file.txt")

# 2. Utilizar la función map para convertir cada línea del RDD en una tupla con tres elementos.
rdd = rdd.map(lambda x: tuple(x.split(",")))

# 3. Utilizar la función reduceByKey para calcular el total de ventas por producto.
ventas_por_producto = rdd.map(lambda x: (x[0], float(x[1]) * int(x[2]))).reduceByKey(lambda x, y: x + y)

# 4. Utilizar la función reduce para calcular el total de ventas para toda la tienda.
total_ventas = rdd.map(lambda x: float(x[1]) * int(x[2])).reduce(lambda x, y: x + y)

# 5. Utilizar la función sortBy para encontrar el producto más vendido.
producto_mas_vendido = ventas_por_producto.sortBy(lambda x: x[1], ascending=False).take(1)

# Imprimir los resultados
print("Total de ventas por producto:")
print(ventas_por_producto.collect())
print("\n")
print("Total de ventas para toda la tienda: " + str(total_ventas) + "$")
print("\n")
print("Producto más vendido: " + producto_mas_vendido[0][0] + " con un total de ventas de " + str(producto_mas_vendido[0][1]) + "$")
