from cassandra.cluster import Cluster
import os
import pandas as pd
from authentication import get_token
from credentials import config, config_frost
import time
import requests

def timer(func):
    def wrapper(*args, **kwargs):
        print(f"{func.__name__} start")
        t1 = time.time()
        result = func(*args, **kwargs)
        t2 = time.time()
        print(f"{func.__name__:-^30} spent {t2-t1:.2f}s")
        return result
    return wrapper


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
    PRIMARY KEY (year, week, localityno));"""

    session.execute(table_creation_query)

def create_localities_table(keyspace="fish_data", reset=False):
    
    session = _initiate_cassandra_driver()
    session.set_keyspace(keyspace)
    if reset:
        session.execute(f"DROP TABLE IF EXISTS locality")
    
    
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
    PRIMARY KEY (year, localityno, week));"""
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

@timer
def get_locality(locality_id, year):
    """Return all localities for a given year from cassandra db

    Args:
        year (int): year that data is requested for

    Returns:
        _type_: dataframe with all data for localities for a given year
    """
    session = _initiate_cassandra_driver()
    session.set_keyspace('fish_data')
    query = f"SELECT * FROM locality WHERE year = {year} AND localityno = {locality_id}"
    rows = session.execute(query)
    print(rows)
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


def finding_closest_station(location_lat, location_lon):
    """Returns the closest weather station to the given location"""
    endpoint = 'https://frost.met.no/sources/v0.jsonld'

    parameters = {
    'elements': 'mean(air_temperature P1D),sum(precipitation_amount P1D),mean(wind_speed P1D)',
    "geometry": f'nearest(POINT({location_lon} {location_lat}))',
    "nearestmaxcount": 1,}
    
    r = requests.get(endpoint, parameters, auth=(config_frost["client_id"],''))
    # Extract JSON data
    json = r.json()
    id = json["data"][0]["id"]
    return id

@timer
def api_weather_station_id(id, year):
    """Returns yearly data for weather id station"""

    year = int(year)
    endpoint = 'https://frost.met.no/observations/v0.jsonld'
    parameters = {
        'sources': f'{id}',
        'elements': 'mean(air_temperature P1D),\
            sum(precipitation_amount P1D),\
            mean(wind_speed P1D),\
            mean(relative_humidity P1D)',
        'referencetime': f'{year}-01-01/{year+1}-01-01'}
    r = requests.get(endpoint, parameters, auth=(config_frost["client_id"],''))

    json = r.json()
    obs_data = json["data"]
    
    df_total = pd.DataFrame()
    for i in range(len(obs_data)):
        row = {}
        for item in obs_data[i]['observations']:
            key = item.get("elementId")
            value = item.get("value")
            if key is not None:
                row[key] = value
        row = pd.DataFrame([row])
        datetime_obj = pd.to_datetime(obs_data[i]["referenceTime"])
        if datetime_obj.month == 1 and datetime_obj.week == 52:
            row['week'] = 0
        else:
            row["week"] = datetime_obj.isocalendar().week
        row["year"] = datetime_obj.year
        row['referenceTime'] = obs_data[i]['referenceTime']
        row['sourceId'] = obs_data[i]['sourceId']
        df_total = pd.concat([df_total, row])

    return df_total


def get_week_summary(token, year, week):
    url = f"{config['api_base_url']}/v1/geodata/fishhealth/locality/{year}/{week}"
    headers ={
    'authorization': 'Bearer ' + token['access_token'],
    'content-type': 'application/json',
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def localities_api(year):
    token = get_token()
    list_localitites = []   
    df_total = pd.DataFrame()
    for week in range(1, 53):
        locality_data = get_week_summary(token, year, week)
        for row in locality_data["localities"]:
            row["year"] = locality_data["year"]
            row["week"] = locality_data["week"]
            list_localitites.append(row)
    df_localities_year = pd.DataFrame(list_localitites)
    df_localities_year.columns = df_localities_year.columns.str.lower()
    return df_localities_year

def localities_api(year):
    list_localitites = []
    df_total = pd.DataFrame()
    for week in range(1, 53):
        locality_data = get_week_summary(token, year, week)
        for row in locality_data["localities"]:
            row["year"] = locality_data["year"]
            row["week"] = locality_data["week"]
            list_localitites.append(row)
    df_localities_year = pd.DataFrame(list_localitites)
    df_localities_year.columns = df_localities_year.columns.str.lower()
    return df_localities_year
