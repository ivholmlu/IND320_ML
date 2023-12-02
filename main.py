import streamsync as ss
import pandas as pd
import plotly.express as px
from use_spark import *
from table_creations import *
from pyspark.sql import SparkSession
from api_creations import *


def _get_init_dataframe(state):
    print("Starting _get_init_dataframe")
    if not state["driver"].check_keyspace_exist("fish_data"):
        state["driver"].create_keyspace("fish_data")
    if not state["driver"].check_table_exist("locality_data"):
        state["driver"].create_locality_table()
    if state["driver"]._check_year("locality_data", 2020):
        state["localities_df"] = state["driver"].get_localities_year(2022)
    _get_localities(state, 2022) # TODO Add IF test to check if table exists
    insert_localities_year(state["localities_df"])
    print("Done with _get_init_dataframe") 

initial_state = ss.init_state({
    "my_app": {
        "title": "Map of norwegian fishing faculties"
    },
    "_my_private_element": 1337,
    "selected":"Click to select",
    "selected_num":-1,
    "current_year" : 2022,
    "driver" : Cassandra_Spark_Driver_Singleton()
})

_get_init_dataframe(initial_state)

