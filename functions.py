from barentswatch.credentials import config
import requests

def get_aquastites(config, token):
    """ Get data from the aquastites using /v1/geodata/fishhealth/localitie

    Args:
        config (dict): Dictionary containing the client_id, base_url and client_secret for the Barentswatch API.
        token (_type_): token from get_token function

    Returns:
        list : List of dictionaries containing the aquastites data
    """
    url = f"{config['api_base_url']}/v1/geodata/fishhealth/localities"
    headers ={
    'authorization': 'Bearer ' + token['access_token'],
    'content-type': 'application/json',}
    r = requests.get(url, headers=headers)
    return r.json()

def get_locality(config, token, year, week):
    """Get data about locality for a given year and week using /v1/geodata/fishhealth/locality/{year}/{week}

    Args:
        year (int): year to extract data
        week (int): week to extract data

    Returns:
        list: list of dictionaries containing the locality data
    """
    url = f"{config['api_base_url']}/v1/geodata/fishhealth/locality/{year}/{week}"
    headers ={
    'authorization': 'Bearer ' + token['access_token'],
    'content-type': 'application/json',}
    r = requests.get(url, headers=headers)
    return r.json()   

def get_year_data_localities(config, token, year, weeks):
    """_summary_

    Args:
        config (_type_): _description_
        token (_type_): _description_
        year (_type_): _description_
        weeks (_type_): _description_

    Returns:
        _type_: _description_
    """
    df_total = pd.DataFrame()
    for week in weeks:
        aquastites = get_locality(config, token, year, week)
        df = pd.DataFrame(aquastites)
        df_expanded = pd.DataFrame(list(df['localities']))
        df_extended = pd.concat([df, df_expanded], axis=1)

        df_total = df_total.append(df_extended, ignore_index=True);
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

def get_detailed_week_summary(token, year, week, locality_id):
    url = f"{config['api_base_url']}/v1/geodata/fishhealth/locality/{locality_id}/{year}/{week}"
    headers ={
    'authorization': 'Bearer ' + token['access_token'],
    'content-type': 'application/json',
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()