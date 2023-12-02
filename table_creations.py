from cassandra.cluster import Cluster
import os
from pyspark.sql import SparkSession
import warnings
from credentials import config, config_frost
from authentication import get_token
import pandas as pd
import requests
class Cassandra_Spark_Driver_Singleton:
    _instance = None

    def __init__(self):
        self.keyspace = "fish_data"
        self.token = get_token()
        self.config = config
        self.config_frost = config_frost

    def _reset_table(self, keyspace, table_name):
        reset_query = f"DROP TABLE IF EXISTS {keyspace}.{table_name};"
        self._session.execute(reset_query)

    def _initiate_cassandra_driver(self, port=9042):
            print("Cassandra initiating...")
            cluster = Cluster(['localhost'], port=port)
            session = cluster.connect()
            session.set_keyspace('fish_data')
            print("Cassandra initiated")
            return session
    def _initiate_spark(self, port='9042'):
        print("Spark initiating...")
        spark = SparkSession.builder.appName('SparkCassandraApp').\
            config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.12:3.4.1').\
            config('spark.cassandra.connection.host', 'localhost').\
            config('spark.sql.extensions', 'com.datastax.spark.connector.CassandraSparkExtensions').\
            config('spark.sql.catalog.mycatalog', 'com.datastax.spark.connector.datasource.CassandraCatalog').\
            config("spark.cassandra.output.batch.size.rows", "1000").\
            config('spark.cassandra.connection.port', port).getOrCreate()
        print("Spark initiated")
        return spark

    def create_locality_table(self,reset=False):
        session = self._initiate_cassandra_driver()
        if reset:
            session.execute(f"DROP TABLE IF EXISTS locality_data")
        session.set_keyspace('fish_data')
        table_creation_query = """CREATE TABLE IF NOT EXISTS locality_data (
        year INT,
        week INT,
        localityno INT,
        localityweekid INT,
        name TEXT,
        hasreportedlice BOOLEAN,
        isfallow BOOLEAN,
        avgadultfemalelice DOUBLE,
        hascleanerfishdeployed BOOLEAN,
        hasmechanicalremoval BOOLEAN,
        hassubstancetreatments BOOLEAN,
        haspd BOOLEAN,
        hasila BOOLEAN,
        municipalityno TEXT,
        municipality TEXT,
        lat DOUBLE,
        lon DOUBLE,
        isonland BOOLEAN,
        infilteredselection BOOLEAN,
        hassalmonoids BOOLEAN,
        isslaughterholdingcage BOOLEAN,
        PRIMARY KEY (year, week, localityno)
    );"""

        self.session.execute(table_creation_query)

    def _create_localities_table(self, reset=False):
        session = self._initiate_cassandra_driver()
        if reset:
            session.execute(f"DROP TABLE IF EXISTS locality_id")
        table_creation_query = f"""CREATE TABLE locality_id (
        year INT,
        week INT,
        localityno INT,
        avgadultfemalelice FLOAT,
        hasreportedlice BOOLEAN,
        avgmobilelice FLOAT,
        avgstationarylice FLOAT,
        seatemperature FLOAT,
        localityname TEXT,
        lat FLOAT,
        lon FLOAT,
        PRIMARY KEY (year, week, localityno)
    );"""
        session.execute(table_creation_query)

    
    def check_table_exist(self, table_to_check):
        session = self._initiate_cassandra_driver()
        print(f"Checking if table {table_to_check} exists")
        tables = session.execute(
            "SELECT table_name FROM system_schema.tables\
            WHERE keyspace_name = %s", [self.keyspace])
        table_names = [row.table_name for row in tables]
        table_exists = False
        if table_to_check in table_names:
            print(f"Table {table_to_check} exists")
            table_exists = True
        else:
            print(f"Table {table_to_check} does not exist")
        return table_exists
    
    def check_keyspace_exist(self, keyspace_to_check):
        print(f"Checking if keyspace {keyspace_to_check} exists")
        session = self._initiate_cassandra_driver()
        keyspaces = session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
        keyspace_names = [row.keyspace_name for row in keyspaces]
        keyspace_exists = False
        if keyspace_to_check in keyspace_names:
            print(f"Keyspace {keyspace_to_check} exists")
            keyspace_exists = True
        else:
            print(f"Keyspace {keyspace_to_check} does not exist")
        return keyspace_exists

    def _create_keyspace(self, keyspace):
        session = self._initiate_cassandra_driver()
        session.execute(
            f"CREATE KEYSPACE IF NOT EXISTS {keyspace} "
            "WITH REPLICATION = {"
            "'class' : 'SimpleStrategy', "
            "'replication_factor' : 1"
            "};"
        )

    def check_year(self, table_name, year):
        session = self._initiate_cassandra_driver()
        print(f"Checking if year {year} exists in table {table_name}")
        query = f"SELECT * FROM {self.keyspace}.{table_name} WHERE year = {year} LIMIT 1;"
        rows = session.execute(query)
        if rows:
            print(f"Year {year} exists in table {table_name}")
            return True
        else:
            print(f"Year {year} does not exist in table {table_name}")
            return False
        
    
    def _get_lat_lon(session, locality_id):
        session.set_keyspace('fish_data')
        query = f"SELECT lat, lon FROM locality_data WHERE year = 2022 AND week = 1 AND localityno = {locality_id}"
        result = session.execute(query)
        
        for row in result:
            lat = row.lat
            lon = row.lon
        return lat, lon
    
    def _get_week_summary(self, year, week):

        url = f"{self.config['api_base_url']}/v1/geodata/fishhealth/locality/{year}/{week}"
        headers ={
            'authorization': 'Bearer ' + self.token['access_token'],
            'content-type': 'application/json',
        }

        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()

    def localities_api(self, year):
        print(f"Getting localities for {year}") # TODO Add IF test to check if table exists
        list_localitites = []
        for week in range(1, 53):
            locality_data = self._get_week_summary(year, week)
            for row in locality_data["localities"]:
                row["year"] = locality_data["year"]
                row["week"] = locality_data["week"]
                list_localitites.append(row)
        df_localities_year = pd.DataFrame(list_localitites)
        df_localities_year.columns = df_localities_year.columns.str.lower()
        print(f"Fetched localities for {year}, returning dataframe")
        return df_localities_year
    
    def get_localities(self, year):
        print(f"_get_localities for {year}")
        df = self.localities_api(year) 
        return df
    
    def insert_localities_year(self, df):
        spark = self._initiate_spark()
    #insert/append into existing table for a yearly instance of data from all localitites.
        print("Inserting into localities table")
        spark_df = spark.createDataFrame(df)
        spark_df.write\
            .format("org.apache.spark.sql.cassandra")\
            .mode('append')\
            .options(table=f"locality_data", keyspace=self.keyspace)\
            .save()
        print("Insertion into localities done")
        spark.stop()


"""     def _initiate_spark_driver(self, port='9042'):
        print("Spark initiating...")
        warnings.simplefilter(action='ignore', category=FutureWarning)
        os.environ["PYSPARK_PYTHON"] = r"/home/ivholmlu/miniconda3/envs/ind320/bin/python" 
        os.environ["PYSPARK_DRIVER_PYTHON"] = r"/home/ivholmlu/miniconda3/envs/ind320/bin/python"   
        self._spark = SparkSession.builder.appName('SparkCassandraApp').\
            config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.12:3.4.1').\
            config('spark.cassandra.connection.host', 'localhost').\
            config('spark.sql.extensions', 'com.datastax.spark.connector.CassandraSparkExtensions').\
            config('spark.sql.catalog.mycatalog', 'com.datastax.spark.connector.datasource.CassandraCatalog').\
            config("spark.cassandra.output.batch.size.rows", "1000").\
            config('spark.cassandra.connection.port', port).getOrCreate()
        print("Spark initiated") """