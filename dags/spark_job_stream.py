from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json,col
from pyspark.sql.types import StringType,StructType,DataType,FloatType,ArrayType,StructField,BooleanType,IntegerType
from kconfig import config
import boto3

from dynamo_class import SendToDynamoDB_ForeachWriter


def read_stream_from_kafka_write_to_dynamodb():
    
    spark_con = SparkSession \
            .builder \
            .appName("LatestMoviedata") \
            .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.13:jar:3.5.0")\
            .getOrCreate()
            
    spark_con.sparkContext.setLogLevel('ERROR')
    
    movie_df = spark_con.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", config['bootstrap.servers'])\
    .option("kafka.security.protocol", "SASL_SSL")\
    .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(config['sasl.username'], config['sasl.password']))\
    .option("kafka.ssl.endpoint.identification.algorithm", "https")\
    .option("kafka.sasl.mechanism", "PLAIN")\
    .option("subscribe", "latest_movie")\
    .option("startingOffsets", "earliest")\
    .option("failOnDataLoss", "false")\
    .load() 
  
    schema = StructType(
       StructField("adult",BooleanType(),False),
       StructField("backdrop_path",BooleanType(),False),
       StructField("genre_ids",ArrayType(),False),
       StructField("id",IntegerType(),False),
       StructField("media_type",StructType(),False),
       StructField("original_language",StringType(),False),
       StructField("original_title",StringType(),False),
       StructField("original_title",StringType(),False),
       StructField("popularity",FloatType(),False),
       StructField("poster_path",StringType(),False),
       StructField("release_date",DataType(),False),
       StructField("video",ArrayType(),False),
       StructField("vote_average",FloatType(),False),
       StructField("vote_count",IntegerType,False),
       )
  
    movies = movie_df.selectExpr("CAST(value AS STRING)")\
   .select(from_json(col('value'),schema).alias('data')).select('data.*')\
   .writeStream\
   .foreach(SendToDynamoDB_ForeachWriter())\
    .outputMode("update")\
    .start()

    return 







                        


    
