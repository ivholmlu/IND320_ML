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
    name = df['name'].values[0]
    fig_hist = px.line(df, x="week", y="avgadultfemalelice", title=f'histogram of avgadultfemalelice for {name}')
    state["plotly_hist"] = fig_hist
    state["test"] = "hist"

def _update_plotly_localities(state):
    print("_update_plotly_localities")
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
    print("Done with _update_plotly_localities")

def handle_click(state, payload):
    print("handle_click")
    print(payload)
    num = payload[0]["pointNumber"] #index of clicked point
    localities = state["localities_df"]
    full_df = state["full_df"]
    locality_no = localities.loc[int(num), 'localityno']
    """
    locality_no = localities.loc[int(num), 'localityno']
    state["localityno"] = locality_no
    df = full_df[full_df['localityno'] == locality_no]
    
    state["click_df"] = df
    state["click_dfshape"] = f"{df.shape}
    """

    #_update_plotly_pd(state)
    #_update_plotly_histogram(state)
    print("Done with handle_click")

    
def handle_select(state, payload):
    print("handle_select")
    print(payload)
    """ state["current_year"] = payload["value"]
    _get_localities(state, payload["value"]) """
    print("Done with handle_select")