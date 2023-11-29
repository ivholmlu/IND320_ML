
import os
from pyspark.sql import SparkSession
import warnings

def initiate_spark():
    
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
        config('spark.cassandra.connection.port', '9042').getOrCreate()
    
    
    warnings.simplefilter(action='default', category=FutureWarning)
    
    return spark