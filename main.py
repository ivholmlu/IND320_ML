import streamsync as ss
import pandas as pd
import plotly.express as px
from use_spark import *
from table_creations import *
from api_creations import *

def _update_plotly_pd(state):
    df = state["click_df"]
    name = df['name'].values[0]
    print(df["haspd"])
    fig_pd = px.pie(df, names="haspd", title=f'Pie chart of hasPd for {name}')
    state["plotly_pd"] = fig_pd
    
def _update_plotly_columns(state):
    df = state["click_df"]
    name = df['name'].values[0]
    col = state["selected:column"]
    fig_columns = px.line(df, x="week", y="avgadultfemalelice", title=f'Line chart of {col} for {name}')
    state["plotly_line"] = fig_columns

def _update_plotly_localities(state):
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

def handle_click(state, payload):
    localities = state["localities_df"]
    full = state["full_df"]
    num = payload[0]["pointNumber"]
    locality_no = localities.loc[num, 'localityno']
    df = full[full['localityno'] == locality_no]
    state["click_df"] = df
    _update_plotly_pd(state) 
    _update_plotly_columns(state)

def handle_select(state, payload):
    print(payload)

def _get_init_dataframes(state):
    if not check_table_exist:
        create_locality_table()
    if not check_year("fish_data", "locality_data", 2022):
        df = localities_api(2022)
        insert_localities_year(df)
        print("Initial dataframe created")
    else:
        df = get_localities(2022) #Via cassandra
        print("Initial dataframe gathered")
    unique_df = df.drop_duplicates(
        subset=['localityno', 'lat', 'lon', 'year', 'name'],
        inplace=False)
    state["full_df"] = df
    state["localities_df"] = unique_df

def _get_JSON(state):
    localitites = state["localities_df"]
    # Create JSON with keys list(range(9)), and restaurant names as values
    my_json = dict(zip(list(range(len(localitites))), localitites["name"].values))
    # Convert keys to strings
    my_json = {str(key): value for key, value in my_json.items()}
    state["localitites_JSON"] = my_json
    
# "_my_private_element" won't be serialised or sent to the frontend,
# because it starts with an underscore (not used here)

initial_state = ss.init_state({
    "my_app": {
        "title": "Map of norwegian fishing faculties"
    },
    "_my_private_element": 1337,
    "selected":"Click to select",
    "selected_num":-1,
    "current_year" : 2022,
})

_get_init_dataframes(initial_state)
_update_plotly_localities(initial_state)
_get_JSON(initial_state)