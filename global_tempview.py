from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.master("Local[*]").appName("Global Temp View").getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    iris = spark.read.option("header",True).csv("iris.csv")
    iris.printSchema()

    iris.createGlobalTempView("iris")
    spark.sql("select * from global_temp.iris").show()

    #New Session and global temp view still exists
    spark.newSession().sql("select * from global_temp.iris").show()   

    spark.stop()
