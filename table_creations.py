from cassandra.cluster import Cluster
import os
import pandas as pd
from authentication import get_token
from credentials import config, config_frost
import time

def timer(func):
    def wrapper():
        print(f"{func.__nmae__"} start")
        t1 = time.time()
        func()
        t2 = time.time()
        print(f"{func.__nmae__"} end, time: {t2-t1}")
    return wrapper

@timer
def _initiate_cassandra_driver(p=9042):
    print("CASSANDRA DRIVER INITIATED")
    cluster = Cluster(['localhost'], port=p)
    session = cluster.connect()
    return session


def reset_table(keyspace, table_name):
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

def create_localities_table(locality_id, keyspace="fish_data", reset=False):
    session = _initiate_cassandra_driver()
    if reset:
        session.execute(f"DROP TABLE IF EXISTS locality")
    session.set_keyspace(keyspace)
    
    table_creation_query = f"""CREATE TABLE locality (
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

def check_table_exist(keyspace, table_to_check):
    session = _initiate_cassandra_driver()
    session.set_keyspace(f'{keyspace}')
    tables = session.execute(f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{keyspace}'")
    table_names = [row.table_name for row in tables]
    table_exists = False
    if table_to_check in table_names:
        table_exists = True
    return table_exists

def create_keyspace(keyspace):
    session = _initiate_cassandra_driver()
    session.execute(
        f"CREATE KEYSPACE IF NOT EXISTS {keyspace} "
        "WITH REPLICATION = {"
        "'class' : 'SimpleStrategy', "
        "'replication_factor' : 1"
        "};"
    )

def check_year(keyspace, table_name, year):
    """Check if year exists in table

    Args:
        keyspace (str): keyspace to check
        table_name (str): table to check
        year (int): year to check

    Returns:
        _type_: True if year exists, else False
    """
    session = _initiate_cassandra_driver()
    query = f"SELECT * FROM {keyspace}.{table_name} WHERE year = {year} ALLOW FILTERING"
    rows = session.execute(query)
    if rows:
        return True
    else:
        return False
    
@timer
def get_localities(year):
    """Return all localities for a given year from cassandra db

    Args:
        year (int): year that data is requested for

    Returns:
        _type_: dataframe with all data for localities for a given year
    """
    session = _initiate_cassandra_driver()
    session.set_keyspace('fish_data')
    query = f"SELECT * FROM locality_data WHERE year = {year}"
    rows = session.execute(query)
    #Return rows as df
    df = pd.DataFrame(list(rows))
    return df

def get_week_summary_locality(token, year, week, localityID):
    url = f"{config['api_base_url']}/v1/geodata/fishhealth/locality/{localityID}/{year}/{week}"
    headers ={
    'authorization': 'Bearer ' + token['access_token'],
    'content-type': 'application/json',
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()  
    
def get_lat_lon(locality_id):
    session = _initiate_cassandra_driver()
    session.set_keyspace('fish_data')
    query = f"SELECT lat, lon FROM locality_data WHERE year = 2022 AND week = 1 AND localityno = {locality_id}"
    result = session.execute(query)
    
    for row in result:
        lat = row.lat
        lon = row.lon
    return lat, lon

@
def localities_api(year):
    token = get_token()
    list_localitites = []
    df_total = pd.DataFrame()
    for week in range(1, 53):
        print(f"localities_api week: {week}, {year}")
        locality_data = get_week_summary(token, year, week)
        for row in locality_data["localities"]:
            row["year"] = locality_data["year"]
            row["week"] = locality_data["week"]
            list_localitites.append(row)
    df_localities_year = pd.DataFrame(list_localitites)
    df_localities_year.columns = df_localities_year.columns.str.lower()
    return df_localities_year

def check_locality_and_year(locality_id, year, keyspace="fish_data", table_name="locality"):
    #Checks for localityno and year, return DataFrame if exists, else return False
    session = _initiate_cassandra_driver()
    session.set_keyspace(keyspace)
    query = f"SELECT * FROM locality WHERE year = {year} AND localityno = {locality_id} ALLOW FILTERING"
    rows = session.execute(query)
    
    if rows:
        df = pd.DataFrame(list(rows))
        return (True, df)
    else:
        return (False, 0)

@timer
def locality_api(localityNo, year):
    values_to_keep = [
        "week", "year", "localityNo", "avgAdultFemaleLice",
        "hasReportedLice", "avgMobileLice", "avgStationaryLice", "seaTemperature"]
    token = get_token()
    df_locality  = pd.DataFrame()
    for week in range(1, 53):
        print(f"locality_api week: {week}, {localityNo} {year}")
        row = {}
        locality_data = get_week_summary_locality(token, year, week, localityNo)
        row["localityname"] = locality_data["localityName"]
        for val in values_to_keep:
            row[val.lower()] = locality_data["localityWeek"][val]
        df_locality = pd.concat([df_locality, pd.DataFrame([row])], ignore_index=True)
    return df_locality


def reset_keyspace():
    session = _initiate_cassandra_driver()
    session.execute("DROP KEYSPACE IF EXISTS fish_data")