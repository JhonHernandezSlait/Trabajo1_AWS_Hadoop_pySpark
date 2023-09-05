from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()
sc = spark.sparkContext

#ACCESO A S3 Y VISUALIZACION DE 5 PRIMERAS LINEAS DE LA TABLA
files_rdd = sc.textFile("s3://climatechange1/raw/capital_cities/*.csv")
primeras_5_lineas = files_rdd.take(5)
for linea in primeras_5_lineas:
    print(linea)

#ACCESO A S3 Y VISUALIZACION DE 10 PRIMERAS LINEAS DE LA TABLA
files_rdd = sc.textFile("s3://climatechange1/raw/greenhouse_emission/*.csv")
primeras_5_lineas = files_rdd.take(10)
for linea in primeras_5_lineas:
    print(linea)

#ACCESO A GLUE Y VISUALIZACION DE DATAFRAME Y HEADS
dataframe = spark.sql("SELECT * FROM raw_db.mountain_ranges")
dataframe
dataframe.head()
