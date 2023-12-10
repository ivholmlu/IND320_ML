import streamsync as ss
import pandas as pd
import plotly.express as px
from use_spark import *
from table_creations import *
import statsmodels.api as sm


def _update_plotly_pd(state):
    print("_update_plotly_pd start")
    df = state["click_df"]
    name = df['name'].values[0]
    fig_pd = px.pie(df, names="haspd", title=f'Pie chart of hasPd for {name} \n{state["current_year"]}')
    state["plotly_pd"] = fig_pd
    print("_update_plotly_pd end")

def merge_weather_and_locality(state):
    print("merge_weather_and_locality start")
    locality_df = state["locality_df"]
    weather_df = state["weather_df"]
    merged_df = locality_df.merge(weather_df, on="week", how="left")
    state["merged_df"] = merged_df
    print("merge_weather_and_locality end")
    print(merged_df.head(3))
    print(merged_df.columns)

def _update_sarimax_table(state):
    merged_df = state["merged_df"]
    col = state["chosen_lice_column"]
    mod = sm.tsa.statespace.SARIMAX(merged_df[col], #merged_df.loc[:, merged_df.columns[3:5]], \
                                trend='c', order=(1,1,1), seasonal_order=(1,1,1,12))
#mod = sm.tsa.statespace.SARIMAX(OilExchange['PerEURO'], OilExchange.loc[:, OilExchange.columns[3:-4]], \
#                                trend='c', order=(1,1,1), seasonal_order=(1,1,1,12))
    res = mod.fit(disp=False)
    print(res.summary()) 
def _update_plotly_columns(state):
    print("_update_plotly_columns start")
    col = state["chosen_column"]
    df = state["click_df"]
    year = state["current_year"]
    name = df['name'].values[0]
    fig_columns = px.line(df, y=col, title=f'Line chart of {col} for {name}\n{year}')
    state["plotly_line"] = fig_columns
    print("_update_plotly_columns end")

def _update_plotly_weather(state):
    print("update_plotly_weather start")
    col = state["chosen_weather"]
    df = state["weather_df"]
    year = state["current_year"]
    fig_columns = px.line(df, x="week", y=col, title=f'Line chart of {col}\n{year}')
    state["plotly_weather"] = fig_columns
    print("update_plotly_weather end")

def handle_weather_select(state, payload):
    print("handle_weather_select start")    
    state["chosen_weather"] = state["weather_JSON"][str(payload)]
    print(state["chosen_column"])
    
    #_update_plotly_columns(state)
    print("handle_weather_select end")
    _update_plotly_weather(state)

def _update_plotly_localities(state):
    print("_update_plotly_localities start")
    localities = state["localities_df"]
    selected_num = state["selected_num"]
    sizes = [10]*len(localities)
    if selected_num != -1:
        sizes[selected_num] = 20
    fig_localities = px.scatter_mapbox(
        localities,
        lat="lat",
        lon="lon",
        hover_name="name",
        hover_data=["lat","lon"],
        color_discrete_sequence=["darkgreen"],
        zoom=7,
        height=600,
        width=700,
    )
    overlay = fig_localities['data'][0]
    overlay['marker']['size'] = sizes
    fig_localities.update_layout(mapbox_style="open-street-map")
    fig_localities.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    state["plotly_localities"] = fig_localities
    print("_update_plotly_localities end")

def _update_plotly_locality_histogram(state):
    #Need to create the df from the locality api first 
    df = state["locality_df"]
    col = state["chosen_lice_column"]
    fig_locality = px.line(df, x = "week", y=col.lower(), title=f"Line plot of {col}")
    fig_locality.add_hline(y=0.7, line_dash="dash", line_color="red")
    state["plotly_locality"] = fig_locality
    #I want a red line through the plot at y = 0.7, and i must know if any values is >= 0.7
    state["show_alarm"] = False
    make_pause = False
    for val in df[col]:
        if val >= 0.7:
            has_over_07 = state["show_alarm"] = True
            make_pause = True
    fig_locality = px.line(df, x = "week", y=col.lower(), title="Line plot of {col}")
    fig_locality.add_hline(y=0.7, line_dash="dash", line_color="red")
    if make_pause:
        time.sleep(4)
    make_pause = False
    state["show_alarm"] = False
    

def handle_click(state, payload):
    print("handle_click start")
    state["running_wheel"] = True
    localities = state["localities_df"]
    full = state["full_df"]
    num = payload[0]["pointNumber"]
    locality_no = localities.loc[num, 'localityno']
    df = full[full['localityno'] == locality_no]
    state["lat"] = df["lat"].values[0]
    state["lon"] = df["lon"].values[0]
    state["chosen_locality"] = int(locality_no)
    state["click_df"] = df
    if not check_locality_and_year(state["chosen_locality"], state["current_year"])[0]:
        locality_df = locality_api(state["chosen_locality"], state["current_year"])
        print(locality_df.head(3))
        insert_into_locality(locality_df)
        state["locality_df"] = locality_df
        print(locality_df.head(3))
    else:
        locality_df = get_locality(state["chosen_locality"], state["current_year"])
        state["locality_df"] = locality_df
        print(locality_df.head(3))
    merge_weather_and_locality(state)
    _update_plotly_pd(state) 
    _update_plotly_columns(state)
    state["running_wheel"] = False
    print("handle_click end")

def handle_select_lice(state, payload):

    state["chosen_lice_column"] = state["lice_JSON"][str(payload)]
    _update_plotly_locality_histogram(state)
    _update_sarimax_table(state)
    print(state["locality_df"])

def handle_select(state, payload):
    print("handle_select start")    
    state["chosen_column"] = state["localities_JSON"][str(payload)]
    _update_plotly_locality_histogram(state)
    print("handle_select end")

def handle_slider(state, payload):
    print("handle_slider start")
    state["running_wheel"] = True
    state["current_year"] = int(payload)
    if not check_year("fish_data", "locality_data", state["current_year"]):
        state["downloading_data"] = True
        full_df = localities_api(state["current_year"])
        insert_localities_year(full_df)
    else:
        full_df = get_localities(state["current_year"] )

    if not check_locality_and_year(state["chosen_locality"], state["current_year"]):
        locality_df = locality_api(state["chosen_locality"], state["current_year"])
        insert_into_locality(locality_df)
        state["locality_df"] = locality_df
    else:
        locality_df = get_locality(state["chosen_locality"], state["current_year"])
        state["locality_df"] = locality_df
    unique_df = full_df.drop_duplicates(
        subset=['localityno', 'lat', 'lon', 'year', 'name'],
        inplace=False)
    locality_no = 30977
    df = full_df[full_df['localityno'] == locality_no]
    state["click_df"] = df
    state["full_df"] = full_df
    state["localities_df"] = unique_df
    state["downloaded_locality_years"].append(state["current_year"])
    state["downloading_data"] = False
    _update_plotly_localities(state)
    _update_plotly_pd(state)
    state["running_wheel"] = False
    print("handle_slider end")

def _get_init_dataframes(state):
    print("_get_init_dataframes start")
    
    if not check_table_exist("fish_data", "locality_data"):
        create_locality_table()
    if not check_year("fish_data", "locality_data", state["current_year"]):
        full_df = localities_api(state["current_year"])
        insert_localities_year(full_df)
    else:
        full_df = get_localities(state["current_year"]) #Via cassandra
        print("Initial dataframe gathered")
    state["downloaded_locality_years"].append(state["current_year"])
    unique_df = full_df.drop_duplicates(
        subset=['localityno', 'lat', 'lon', 'year', 'name'],
        inplace=False)
    state["chosen_locality"] = 30977
    #create_localities_table(reset=True) #TODO TB removed
    if not check_table_exist("fish_data", "locality"):
        create_localities_table(reset=True)
    if not check_locality_and_year(state["chosen_locality"], state["current_year"])[0]:
        locality_df = locality_api(state["chosen_locality"], state["current_year"])
        insert_into_locality(locality_df)
        state["locality_df"] = locality_df
        print(locality_df.head(3))
    else:
        locality_df = get_locality(state["chosen_locality"], state["current_year"])
        state["locality_df"] = locality_df
        print(locality_df.head(3))
    df = full_df[full_df['localityno'] == state["chosen_locality"]]
    state["click_df"] = df
    state["lat"] = df["lat"].values[0]
    state["lon"] = df["lon"].values[0]
    print(state["lat"], state["lon"])
    state["full_df"] = full_df
    state["localities_df"] = unique_df
    state["weather_id"] = finding_closest_station(state["lat"], state["lon"])
    weather_df = api_weather_station_id(state["weather_id"], state["current_year"])
    state["weather_df"] = weather_df
    #Picvot on week, and take mean of all columns
    weather_df = weather_df.pivot_table(index="week", values=weather_df.columns[0:3], aggfunc="mean")
    state["weather_df"] = weather_df
    print(state["weather_df"].head(3))

    print("_get_init_dataframes end")

def reset_fish_data(state):

    state["running_wheel"] = True
    session = _initiate_cassandra_driver()
    session.execute("DROP KEYSPACE IF EXISTS fish_data")
    create_locality_table()
    full_df = localities_api(2022)
    insert_localities_year(full_df)
    state["downloaded_locality_years"] = []
    state["downloaded_locality_years"].append(2022)
    unique_df = full_df.drop_duplicates(
        subset=['localityno', 'lat', 'lon', 'year', 'name'],
        inplace=False)
    locality_no = 30977
    df = full_df[full_df['localityno'] == locality_no]
    state["click_df"] = df
    state["full_df"] = full_df
    state["localities_df"] = unique_df
    _update_plotly_localities(state)
    _update_plotly_pd(state)
    state["chosen_column"] = "avgadultfemalelice"
    _update_plotly_columns(state)
    state["running_wheel"] = False

def _get_JSON(state):
    print("_get_JSON start")
    my_json = dict(zip(list(range(len(state["l_columns"]))), state["l_columns"]))
    my_json = {str(key): value for key, value in my_json.items()}
    lice_cols = ["avgadultfemalelice", "hasreportedlice", "avgmobilelice", "avgstationarylice"]
    lice_json = dict(zip(list(range(len(lice_cols))), lice_cols))
    lice_json = {str(key): value for key, value in lice_json.items()}
    weather_cols = ["mean(air_temperature P1D)", "sum(precipitation_amount P1D)", "mean(wind_speed P1D)", "mean(relative_humidity P1D)"]
    weather_json = dict(zip(list(range(len(weather_cols))), weather_cols))
    weather_json = {str(key): value for key, value in weather_json.items()}
    state["lice_JSON"] = lice_json
    state["localities_JSON"] = my_json
    state["weather_JSON"] = weather_json

    print("_get_JSON end")

initial_state = ss.init_state({
    "my_app": {
        "title": "Map of norwegian fishing faculties"
    },
    "running_wheel" : False,
    "_my_private_element": 1337,
    "selected":"Click to select",
    "selected_num":-1,
    "current_year" : 2022,
    "l_columns" : [
        "avgadultfemalelice", "hasreportedlice",
        "hassalmonoids", "hasmechanicalremoval"],
    "chosen_column" : "avgadultfemalelice",
    "downloaded_locality_years" : [],
    "chosen_lice_column" : "avgAdultFemaleLice",
        })

_get_init_dataframes(initial_state)
_update_plotly_localities(initial_state)
_update_plotly_columns(initial_state)
_update_plotly_pd(initial_state)
_get_JSON(initial_state)

