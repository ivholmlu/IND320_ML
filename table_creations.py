from cassandra.cluster import Cluster

def reset_table(session, keyspace, table_name):
    reset_query = f"DROP TABLE IF EXISTS {keyspace}.{table_name};"
    session.execute(reset_query)

def create_locality_table(session):

    session.set_keyspace('fish_data')
    table_creation_query = """
        CREATE TABLE IF NOT EXISTS locality_data (
            year INT,
            week INT,
            localityNo INT,
            localityWeekId INT PRIMARY KEY,
            name TEXT,
            hasReportedLice BOOLEAN,
            isFallow BOOLEAN,
            avgAdultFemaleLice DOUBLE,
            hasCleanerfishDeployed BOOLEAN,
            hasMechanicalRemoval BOOLEAN,
            hasSubstanceTreatments BOOLEAN,
            hasPd BOOLEAN,
            hasIla BOOLEAN,
            municipalityNo TEXT,
            municipality TEXT,
            lat DOUBLE,
            lon DOUBLE,
            isOnLand BOOLEAN,
            inFilteredSelection BOOLEAN,
            hasSalmonoids BOOLEAN,
            isSlaughterHoldingCage BOOLEAN
        );
    """
    session.execute(table_creation_query)

def create_localities_table(session):

    session.execute("DROP TABLE locality_{locality_id}")
    table_creation_query = """
        CREATE TABLE locality_{locality_id} (
            datetime TEXT PRIMARY KEY,
            avgAdultFemaleLice FLOAT,
            hasReportedLice BOOLEAN,
            avgMobileLice FLOAT,
            avgStationaryLice FLOAT,
            seaTemperature FLOAT,
        )
        """
    session.execute(table_creation_query)

def check_table_exist(session, keyspace):
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

def initiate_spark():
    pass
