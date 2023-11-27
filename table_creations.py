

def reset_table(session, keyspace, table_name):
    reset_query = f"DROP TABLE {keyspace}.{table_name};"
    session.execute(reset_query)

def create_locality_table(session):

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
    
def create_locality_table(session):
    session.execute("DROP TABLE id_35297")

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