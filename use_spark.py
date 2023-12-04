
import os
from pyspark.sql import SparkSession
import warnings

def _initiate_spark(port='9042'):
    print("SSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS")
    warnings.simplefilter(action='ignore', category=FutureWarning)
    from pyspark.sql import SparkSession
    os.environ["PYSPARK_PYTHON"] = r"/home/ivholmlu/miniconda3/envs/ind320/bin/python" 
    os.environ["PYSPARK_DRIVER_PYTHON"] = r"/home/ivholmlu/miniconda3/envs/ind320/bin/python"   
    spark = SparkSession.builder.appName('SparkCassandraApp').\
        config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.12:3.4.1').\
        config('spark.cassandra.connection.host', 'localhost').\
        config('spark.sql.extensions', 'com.datastax.spark.connector.CassandraSparkExtensions').\
        config('spark.sql.catalog.mycatalog', 'com.datastax.spark.connector.datasource.CassandraCatalog').\
        config("spark.cassandra.output.batch.size.rows", "1000").\
        config('spark.cassandra.connection.port', port).getOrCreate()
    
    
    warnings.simplefilter(action='default', category=FutureWarning)
    
    return spark

def insert_into_locality(spark, locality_id, df, keyspace="fish_data"):
    #insert/append into existing table for single instance of locality data
    spark = _initiate_spark()
    spark_df = spark.createDataFrame(df)
    spark_df.write\
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(table=f"locality", keyspace=keyspace)\
        .save()
    spark.stop()

def insert_localities_year(df, keyspace="fish_data"):
    #insert/append into existing table for a yearly instance of data from all localitites.
    spark = _initiate_spark()
    spark_df = spark.createDataFrame(df)
    spark_df.write\
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(table=f"locality_data", keyspace=keyspace)\
        .save()
    spark.stop()