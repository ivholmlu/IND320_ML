import streamsync as ss
import pandas as pd
import plotly.express as px
from use_spark import *
from table_creations import *
import os
from pyspark.sql import SparkSession
from api_creations import *


"""
def _update_plotly_pd(state):
    df = state["click_df"]
    name = df['name'].values[0]
    pie_data = df['haspd'].value_counts().reset_index()
    pie_data.columns = ['haspd', 'count']
    fig_pd = px.pie(pie_data, names="haspd", values='count', title=f'Pie chart of hasPd for {name}')
    state["plotly_pd"] = fig_pd
    print(fig_pd) """


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

""" def handle_click(state, payload):
    localities = state["localities_df"]
    num = payload[0]["pointNumber"]
    locality_no = localities.loc[num, 'localityno']
    df = localities[localities['localityno'] == locality_no]
    state["click_df"] = df
    state["histo_columns"] = df.select_dtypes(include=['int', 'float']).columns
    _update_plotly_pd(state) """

def _get_init_dataframe(state):
    create_locality_table()
    df = localities_api(2022)
    state["localities_df"] = df
    insert_localities_year(state["localities_df"])


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

_get_init_dataframe(initial_state)
_update_plotly_localities(initial_state)