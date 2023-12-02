import streamsync as ss
import pandas as pd
import plotly.express as px
from use_spark import *
from table_creations import *
from api_creations import *

def _update_plotly_pd(state):
    print("_update_plotly_pd start")
    df = state["click_df"]
    name = df['name'].values[0]
    fig_pd = px.pie(df, names="haspd", title=f'Pie chart of hasPd for {name} \n{state["current_year"]}')
    state["plotly_pd"] = fig_pd
    print("_update_plotly_pd end")
    
def _update_plotly_columns(state):
    print("_update_plotly_columns start")
    col = state["chosen_column"]
    df = state["click_df"]
    year = state["current_year"]
    name = df['name'].values[0]
    fig_columns = px.line(df, x="week", y=col, title=f'Line chart of {col} for {name}\n{year}')
    state["plotly_line"] = fig_columns
    print("_update_plotly_columns end")

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
        zoom=5,
        height=600,
        width=700,
    )
    overlay = fig_localities['data'][0]
    overlay['marker']['size'] = sizes
    fig_localities.update_layout(mapbox_style="open-street-map")
    fig_localities.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    state["plotly_localities"] = fig_localities
    print("_update_plotly_localities end")

def handle_click(state, payload):
    print("handle_click start")
    localities = state["localities_df"]
    full = state["full_df"]
    num = payload[0]["pointNumber"]
    locality_no = localities.loc[num, 'localityno']
    df = full[full['localityno'] == locality_no]
    state["click_df"] = df
    _update_plotly_pd(state) 
    _update_plotly_columns(state)
    print("handle_click end")
    
def handle_select_lice(state, payload):
    pass
def handle_select(state, payload):
    print("handle_select start")    
    state["chosen_column"] = state["localities_JSON"][str(payload)]
    print(state["chosen_column"])
    _update_plotly_columns(state)
    print("handle_select end")

def handle_slider(state, payload):
    print("handle_slider start")
    state["running_wheel"] = True
    state["current_year"] = int(payload)
    if not check_year("fish_data", "locality_data", state["current_year"]):
        full_df = localities_api(state["current_year"])
        insert_localities_year(full_df)
    else:
        full_df = get_localities(state["current_year"] )
    unique_df = full_df.drop_duplicates(
        subset=['localityno', 'lat', 'lon', 'year', 'name'],
        inplace=False)
    locality_no = 30977
    df = full_df[full_df['localityno'] == locality_no]
    state["click_df"] = df
    state["full_df"] = full_df
    state["localities_df"] = unique_df
    state["downloaded_locality_years"].append(state["current_year"])
    _update_plotly_localities(state)
    _update_plotly_pd(state)
    state["running_wheel"] = False
    print("handle_slider end")

def _get_init_dataframes(state):
    print("_get_init_dataframes start")
    if not check_table_exist("fish_data", "locality_data"):
        create_locality_table()
    if not check_year("fish_data", "locality_data", 2022):
        full_df = localities_api(2022)
        insert_localities_year(full_df)
    else:
        full_df = get_localities(2022) #Via cassandra
        print("Initial dataframe gathered")
    state["downloaded_locality_years"].append(2022)
    unique_df = full_df.drop_duplicates(
        subset=['localityno', 'lat', 'lon', 'year', 'name'],
        inplace=False)
    
    locality_no = 30977
    df = full_df[full_df['localityno'] == locality_no]
    state["click_df"] = df
    state["full_df"] = full_df
    state["localities_df"] = unique_df
    print("_get_init_dataframes end")

def _get_JSON(state):
    print("_get_JSON start")
    my_json = dict(zip(list(range(len(state["l_columns"]))), state["l_columns"]))
    my_json = {str(key): value for key, value in my_json.items()}
    state["localities_JSON"] = my_json
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
    "downloaded_locality_years" : []
        })

_get_init_dataframes(initial_state)
_update_plotly_localities(initial_state)
_update_plotly_columns(initial_state)
_update_plotly_pd(initial_state)
_get_JSON(initial_state)

