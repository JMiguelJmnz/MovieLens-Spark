from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MovieLensTest").getOrCreate()

df = spark.read.csv("ml-latest-small/movies.csv", header=True, inferSchema=True)
df.show(5)

spark.stop()