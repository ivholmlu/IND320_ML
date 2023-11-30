import streamsync as ss
import pandas as pd
import plotly.express as px
from use_spark import *
from table_creations import *
import os
from pyspark.sql import SparkSession
from api_creations import *

def _update_plotly_pd(state):
    df = state["click_df"]
    name = df['name'].values[0]
    pie_data = df['haspd'].value_counts().reset_index()
    pie_data.columns = ['haspd', 'count']
    fig_pd = px.pie(pie_data, names="haspd", values='count', title=f'Pie chart of hasPd for {name}')
    state["plotly_pd"] = fig_pd
    state["test"] = "pd"
    
def _update_plotly_histogram(state):
    df = state["click_df"]
    print(df["avgadultfemalelice"].values)
    name = df['name'].values[0]
    fig_hist = px.line(df, x="week", y="avgadultfemalelice", title=f'histogram of avgadultfemalelice for {name}')
    state["plotly_hist"] = fig_hist
    state["test"] = "hist"

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
        hover_data=["lat","lon","localityno"],
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
    full_df = state["full_df"]
    num = payload[0]["pointNumber"] #index of clicked point
    locality_no = localities.loc[num, 'localityno']
    state["localityno"] = locality_no
    df = full_df[full_df['localityno'] == locality_no]
    state["click_df"] = df
    state["click_dfshape"] = f"{df.shape}"

    _update_plotly_pd(state)
    _update_plotly_histogram(state)


    
def _update_year(state):
    df = localities_api(state["current_year"])
    state["full_df"] = df 
    state["full_dfshape"] = f"{df.shape}"
    unique_df =  df.drop_duplicates(subset=['localityno', 'lat', 'lon', 'year', 'name'], inplace=False) #Values needed in map plot
    state["localities_df"] = unique_df
    state["localities_dfshape"] = f"{unique_df.shape}"
    _update_plotly_localities(state)

def handle_select(state, payload):
    state["current_year"] = payload["value"]
__  update_year(state)

def _get_init_dataframe(state):
    create_locality_table()
    df = localities_api(2022)
    state["full_df"] = df 
    state["full_dfshape"] = f"{df.shape}"
    unique_df =  df.drop_duplicates(subset=['localityno', 'lat', 'lon', 'year', 'name'], inplace=False) #Values needed in map plot
    state["localities_df"] = unique_df
    state["localities_dfshape"] = f"{unique_df.shape}"
    insert_localities_year(state["localities_df"])


initial_state = ss.init_state({
    "my_app": {
        "title": "Map of norwegian fishing faculties"
    },
    "_my_private_element": 1337,
    "selected":"Click to select",
    "selected_num":-1,
    "current_year" : 2022,
})

_get_init_dataframe(initial_state)
_update_plotly_localities(initial_state)