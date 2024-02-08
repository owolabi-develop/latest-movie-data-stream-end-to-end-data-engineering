

def stream_from_spark():
    import pyspark
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("local[*]") \
                        .appName('latestmovies') \
                        .getOrCreate()


    df = spark.createDataFrame(
        [("Scala", 25000), ("Spark", 35000), ("PHP", 21000)])
    
    
    return df.show()