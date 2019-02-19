from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.master('Local[*]').appName("Hello Spark SQL").getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    df = spark.read.json('example_1.json')
    df.createOrReplaceTempView("fruits")

    fruits_df = spark.sql("select * from fruits where size == 'Medium'")
    fruits_df.show()

    spark.sql("select count(*) from fruits").show()
    print(df.count())

    spark.stop()
