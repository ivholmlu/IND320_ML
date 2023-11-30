from cassandra.cluster import Cluster
import os

def _initiate_cassandra_driver(p=9042):
    print("CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC")
    cluster = Cluster(['localhost'], port=p)
    session = cluster.connect()
    return session

def reset_table(session, keyspace, table_name):
    session = _initiate_cassandra_driver()
    reset_query = f"DROP TABLE IF EXISTS {keyspace}.{table_name};"
    session.execute(reset_query)

def create_locality_table(session=1):
    session = _initiate_cassandra_driver()
    session.set_keyspace('fish_data')
    session.execute(f"DROP TABLE IF EXISTS locality_data")
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

    session.execute(table_creation_query)

def create_localities_table(session, locality_id, keyspace="fish_data"):
    session.set_keyspace(keyspace)
    session.execute(f"DROP TABLE IF EXISTS locality_{locality_id}")
    table_creation_query = f"""CREATE TABLE locality_{locality_id} (
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

def check_table_exist(session, keyspace, table_to_check):
    session.set_keyspace('{keyspace}')
    tables = session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s", [keyspace])
    table_names = [row.table_name for row in tables]
    table_exists = False
    if table_to_check in table_names:
        table_exists = True
    return table_exists

def create_keyspace(session, keyspace):
    session.execute(
        f"CREATE KEYSPACE IF NOT EXISTS {keyspace} "
        "WITH REPLICATION = {"
        "'class' : 'SimpleStrategy', "
        "'replication_factor' : 1"
        "};"
    )

def initiate_cassandra_driver(p=9042):
    cluster = Cluster(['localhost'], port=p)
    session = cluster.connect() 
    return session

def check_year(session, keyspace, table_name, year):
    query = f"SELECT * FROM {keyspace}.{table_name} WHERE year = {year} ALLOW FILTERING"
    rows = session.execute(query)
    if rows:
        return True
    else:
        return False
    
def get_lat_lon(session, locality_id):
    session.set_keyspace('fish_data')
    query = f"SELECT lat, lon FROM locality_data WHERE year = 2022 AND week = 1 AND localityno = {locality_id}"
    result = session.execute(query)
    
    for row in result:
        lat = row.lat
        lon = row.lon
    return lat, lon
