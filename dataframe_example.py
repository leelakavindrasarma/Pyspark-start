from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("Hello Dataframe").getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    
    df = spark.read.json('example_1.json')
    df.printSchema()
    df.show()

    df.select(df['fruit'],df['color']).show()
    df.filter(df['color'] == 'Yellow').show()
    df.filter(df['size'] == 'Large').show()

    spark.stop()
