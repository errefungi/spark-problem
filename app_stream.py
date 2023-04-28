#ds.py
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
# Inicializamos contexto
sc=SparkContext("local[2]","NetworkCount")
ssc=StreamingContext(sc,10)
lines=ssc.socketTextStream("localhost",9090)
#Proceso de datos
words=lines.flatMap(lambda line:line.split(" "))
pairs=words.map(lambda word:(word,1))
wordCounts=pairs.reduceByKey(lambda x,y:x+y)
wordCounts.pprint()
ssc.start()
ssc.awaitTermination()
