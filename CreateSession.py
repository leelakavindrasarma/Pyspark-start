from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.master("local").appName("Hello, Spark Session").getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    print(spark.sparkContext.appName)
    spark.stop()
