from pyspark.sql import SparkSession
from pyspark.sql.functions import col,min,max,avg

if __name__ == "__main__":
    spark = SparkSession.builder.master('Local[*]').appName("Spark Aggregations").getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    df = spark.read.json('example_1.json')
    df.printSchema()

    df_fruit = df.select(col("Rating").alias("Numbers"),col("fruit").alias("Fruits"))
    df_fruit.printSchema()
    df_fruit.show()

    df.select(avg("Rating")).show()
    df.select(min("Rating")).show()
    df.select(max("Rating")).show()

    spark.stop()
