from cassandra.cluster import Cluster
import os
from pyspark.sql import SparkSession
import warnings
from credentials import config, config_frost
from authentication import get_token
import pandas as pd
class Cassandra_Spark_Driver_Singleton:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Cassandra_Spark_Driver_Singleton, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, 'initialized'):  # Ensure initialization happens only once
            self.keyspace = "fish_data"
            self.token = get_token()
            self.config = config
            self.config_frost = config_frost
            self._initiate_spark_driver()
            self._initiate_cassandra_driver()
            self.initialized = True

    def _initiate_cassandra_driver(self, port=9042):
            print("Cassandra initiating...")
            cluster = Cluster(['localhost'], port=port)
            self._session = cluster.connect()
            self._session.set_keyspace('fish_data')
            print("Cassandra initiated")
    
    def _initiate_spark_driver(self, port='9042'):
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
        print("Spark initiated")
        

    def _reset_table(cls, keyspace, table_name):
        reset_query = f"DROP TABLE IF EXISTS {keyspace}.{table_name};"
        cls.session.execute(reset_query)

    def create_locality_table(self, reset=False):
        if reset:
            self._session.execute(f"DROP TABLE IF EXISTS locality_data")
        self.session.set_keyspace('fish_data')
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
        if reset:
            self.session.execute(f"DROP TABLE IF EXISTS locality_id")
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
        self.session.execute(table_creation_query)

    def check_table_exist(self, table_to_check):
        print(f"Checking if table {table_to_check} exists")
        tables = self._session.execute(
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
        keyspaces = self._session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
        keyspace_names = [row.keyspace_name for row in keyspaces]
        keyspace_exists = False
        if keyspace_to_check in keyspace_names:
            print(f"Keyspace {keyspace_to_check} exists")
            keyspace_exists = True
        else:
            print(f"Keyspace {keyspace_to_check} does not exist")
        return keyspace_exists

    def _create_keyspace(self, keyspace):
        self._session.execute(
            f"CREATE KEYSPACE IF NOT EXISTS {keyspace} "
            "WITH REPLICATION = {"
            "'class' : 'SimpleStrategy', "
            "'replication_factor' : 1"
            "};"
        )

    def _check_year(cls, keyspace, table_name, year):
        query = f"SELECT * FROM {keyspace}.{table_name} WHERE year = {year} LIMIT 1;"
        rows = cls.session.execute(query)
        if rows:
            return True
        else:
            return False
    
    def _get_lat_lon(session, locality_id):
        session.set_keyspace('fish_data')
        query = f"SELECT lat, lon FROM locality_data WHERE year = 2022 AND week = 1 AND localityno = {locality_id}"
        result = session.execute(query)
        
        for row in result:
            lat = row.lat
            lon = row.lon
        return lat, lon
    
    def _get_week_summary(self):

        url = f"{self.config['api_base_url']}/v1/geodata/fishhealth/locality/{year}/{week}"
        headers ={
            'authorization': 'Bearer ' + self.token['access_token'],
            'content-type': 'application/json',
        }

        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()

    def localities_api(year):
        print(f"Getting localities for {year}") # TODO Add IF test to check if table exists
        token = get_token()
        list_localitites = []
        for week in range(1, 53):
            locality_data = _get_week_summary(year, week)
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
        df = self.localities_api(year) # TODO Add IF test to check if table exists
        