import requests
import pandas as pd
from credentials import config_frost
from credentials import config
from authentication import get_token


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


def _get_week_summary(token, year, week):
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
        locality_data = _get_week_summary(token, year, week)
        for row in locality_data["localities"]:
            row["year"] = locality_data["year"]
            row["week"] = locality_data["week"]
            list_localitites.append(row)
    df_localities_year = pd.DataFrame(list_localitites)
    df_localities_year.columns = df_localities_year.columns.str.lower()
    return df_localities_year