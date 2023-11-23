import streamsync as ss
import pandas as pd
import plotly.express as px
#from functions import get_weekly_summary

# Its name starts with _, so this function won't be exposed

# Import data
def _get_main_df():
    main_df = pd.read_csv('../../data/localities.csv')
    return main_df

def _get_localities_year(state):
    localities_df = state["localities_df"]
    return localities

# Plot restaurants
def _update_plotly_localities(state):
    localities = state["localities_df"]
    selected_num = state["selected_num"]
    sizes = [10]*len(localities)
    if selected_num != -1:
        sizes[selected_num] = 20
    fig_restaurants = px.scatter_mapbox(
        localities,
        lat="lat",
        lon="lon",
        hover_name="name",
        hover_data=["lat","lon"],
        color_discrete_sequence=["darkgreen"],
        zoom=3,
        height=600,
        width=700,
    )
    overlay = fig_restaurants['data'][0]
    overlay['marker']['size'] = sizes
    fig_restaurants.update_layout(mapbox_style="open-street-map")
    fig_restaurants.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    state["plotly_localities"] = fig_restaurants

def handle_click(state, payload):
    restaurants = state["restaurants_df"]
    state["selected"] = restaurants["name"].values[payload[0]["pointNumber"]]
    state["selected_num"] = payload[0]["pointNumber"]
    _update_plotly_restaurants(state)



# Initialise the state

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
    "localities_df":_get_main_df(),
})

_update_plotly_localities(initial_state)

