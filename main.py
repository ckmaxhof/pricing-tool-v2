import sys
sys.path.append('/home/maximilian.hofmann/tools/brand_science_pricing')
sys.path.append('/home/maximilian.hofmann/ff_utils/src')

import constants as const
import functions as fun
import maps_utils as mu

import numpy as np
import pandas as pd
import streamlit as st
from streamlit_folium import folium_static
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
import folium
from folium.plugins import MarkerCluster
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.figure_factory as ff
from pandasql import sqldf
import math

import nltk
nltk.download('punkt') 

st.set_page_config(
    page_title="Pricing Tool",
    page_icon='dollar',
    layout='wide'
)

# user_id, user_email = login_info
user_actions = {}
user_actions['tool_name'] = 'brand_science_pricing'
# user_actions['user_email'] = user_email
# user_actions['user_id'] = user_id

primary_cuisines = fun.load_primary_cuisine()
cities_df = fun.load_cities()

#####################
### Text Block
#####################
st.title('Pricing Tool | Brand Science')
st.markdown('This tool gives a hollistic overview of setting an initial price for a menu item in a given geography. We are combining marketintel data (OFO scrapes) and Otter data in order to calculate an effective price.')
st.markdown('You can check current data coverage in [this sheet](https://docs.google.com/spreadsheets/d/1gxB_60JSDvb2nVObVfSFB66qfWRIFyyNoAcEh-AvmcU/edit?usp=sharing).')
st.markdown('More details on the methodology can be found [here](https://docs.google.com/document/d/1VODjLh1_7F_2lE0IhuTGQRVaga546Vy7Gy4WLNbDwVE/edit#heading=h.8n2n4oy69nrt).')
st.markdown('We need your feedback! Please use [this form](https://forms.gle/mZmqGKw5Q1bTMa97A) to submit your feature requests, bug reports and wishes.')
st.markdown('If you have any questions, please reach out to @maximilian.hofmann.')

#####################
### User Inputs
#####################  

city_col, country_col, radius_col = st.columns(3)

cities_input = city_col.multiselect(
    label='Cities',
    options=cities_df['city'].unique(),
    default='London',
    on_change=fun.set_session_state_0
)

country_codes_input = country_col.multiselect(
    label='Country Codes',
    options=cities_df[cities_df['city'].isin(cities_input)]['iso2'].unique(),
    on_change=fun.set_session_state_0,
    default='GB'
)

radius_input = radius_col.number_input('Radius (in km)', min_value=0, value=10, on_change=fun.set_session_state_0)
radius_input = radius_input * 1000

search_option_col, search_string_col = st.columns([2, 8])
search_method = search_option_col.radio('String Search Method', ('Contains', 'Equals'), on_change=fun.set_session_state_0)
search_string_inputs = search_string_col.text_input('Please type in the items you are looking for. If multiple, please separate by comma.', on_change=fun.set_session_state_0, value='chicken tikka masala')

with st.expander("Additional Filters"):

    exclude_string_input = st.text_input(
        'Exclude specific words. If multiple, please separate by comma.'
    )

    exclude_primary_cuisine_col, exclude_store_names_col = st.columns(2)
    with exclude_primary_cuisine_col:
        exclude_primary_cuisine = st.multiselect(
            label='Exclude primary cuisine tags (OFO scrape data)',
            options=primary_cuisines,
            default=None
        )
    with exclude_store_names_col:
        exclude_store_names = st.text_input(
            label='Exclude store names / brand names'
        )
    include_primary_cuisine = st.multiselect(
        label='Include primary cuisine tags (OFO scrape data)',
        options=primary_cuisines,
        default=None
    )

show_map = st.checkbox('MAP ME OUT!')

b_col_1, b_col_2 = st.columns(2)
b = b_col_1.button('Run Query')
reset = b_col_2.button('Reset App')

if 's' not in st.session_state:
    st.session_state.s = 0

if b:
    st.session_state.s += 1

if reset:
    st.session_state.s = 0

if st.session_state.s > 0: 

    cities_df_filtered = cities_df[(cities_df['city'].isin(cities_input))&(cities_df['iso2'].isin(country_codes_input))]

    cities_df_filtered = cities_df_filtered[['iso2','city','lat','lng']].drop_duplicates()

    geo_filter_list = list(zip(cities_df_filtered['iso2'], cities_df_filtered['city'], cities_df_filtered['lat'], cities_df_filtered['lng']))
    lats = cities_df_filtered['lat'].tolist()
    lngs = cities_df_filtered['lng'].tolist()
    country_codes = cities_df_filtered['iso2'].unique().tolist()

    # LOG ACTION
    query_args = {
        'testing':True,
        'geo_filter_list': geo_filter_list,
        'radius': radius_input,
        'search_item_list': search_string_inputs,
        'exclude_string_input': exclude_string_input,
        'exclude_primary_cuisine': exclude_primary_cuisine,
        'include_primary_cuisine': include_primary_cuisine,
        'exclude_store_names': exclude_store_names
    }
    # fun.log_user_actions(user_actions, 'run_query', json.dumps(query_args))

    search_item_list = search_string_inputs.split(',')

    tabs = st.tabs(search_item_list)

    for i, search_string_input in enumerate(search_item_list):

        m = folium.Map(location=[lats[0], lngs[0]], zoom_start=10)
        for lat, lng in zip(lats, lngs):
            folium.Circle([lat, lng], radius=radius_input).add_to(m)

        try:

            ### CALCULATIONS
            ### Supply
            ofo_df = fun.load_ofo_data(
                fun.get_country_codes_filter(country_codes), 
                fun.get_st_distance_filters(lats, lngs, radius_input),
                fun.get_search_term_query(search_string_input, search_method), 
                fun.get_exclude_term_query(exclude_string_input), 
                fun.get_exclude_primary_cuisines_query(exclude_primary_cuisine),
                fun.get_include_primary_cuisines_query(include_primary_cuisine),
                fun.get_exclude_store_names_query(exclude_store_names)
                ) 

            ### Demand
            otter_df = fun.load_otter_data(
                fun.get_country_codes_filter(country_codes), 
                fun.get_st_distance_filters(lats, lngs, radius_input),
                fun.get_search_term_query(search_string_input, search_method), 
                fun.get_exclude_term_query(exclude_string_input), 
                fun.get_exclude_brand_names_query(exclude_store_names)
            )

            histogram_df = pd.DataFrame()


            date_list = np.unique(ofo_df['month'].tolist() + otter_df['month'].tolist())
            date_list = [pd.to_datetime(x).strftime('%Y-%m-%d') for x in date_list]

            c1, c2 = tabs[i].columns([4,8])

            with c1:
                c1.markdown('### Select a date range for the data')
                start_date, end_date = c1.select_slider(
                    label='Date Range',
                    label_visibility='collapsed',
                    options=date_list,
                    value=(date_list[0], date_list[-1])
                )

            ofo_df = ofo_df[(ofo_df['month']>=start_date)&(ofo_df['month']<=end_date)]
            otter_df = otter_df[(otter_df['month']>=pd.to_datetime(start_date))&(otter_df['month']<=pd.to_datetime(end_date))]

            if ofo_df.shape[0] > 0:
                ofo_mean = ofo_df.price.mean()
                ofo_median = ofo_df.price.median()
                ofo_max_month = ofo_df['month'].max()
                ofo_min_month = ofo_df['month'].min()

                ofo_plot_df = ofo_df.groupby(['month']).agg(
                    clean_name=("clean_name", "nunique"),
                    price=("price", "mean")
                ).reset_index(drop=False)

                ofo_histogram = px.histogram(ofo_df, x='price')
                ofo_histogram_df = ofo_df[['item_name', 'price']]
                ofo_histogram_df = ofo_histogram_df.assign(data_source='OFO Scrape')

                ofo_recent_updates_by_item_df = ofo_df.groupby(['store_name','item_name']).agg({'month':'max'}).reset_index(drop=False)
                ofo_recent_prices_df = pd.merge(ofo_df, ofo_recent_updates_by_item_df, on=['store_name','item_name','month'], how='inner')

                ofo_recent_prices_df = ofo_recent_prices_df.assign(data_source='OFO Scrape')

                histogram_df = pd.concat([histogram_df, ofo_recent_prices_df[['item_name','price','data_source']]])
                # histogram_df = pd.concat([histogram_df, ofo_histogram_df])

                ofo_df = ofo_df.dropna()

                if show_map:
                    marker_cluster = MarkerCluster(icon_create_function=mu.icon_create_function)
                    marker_data = zip(ofo_df['latitude'], ofo_df['longitude'], ofo_df['price'], ofo_df['item_name'], ofo_df['store_name'], ofo_df['primary_cuisine'])
                    for lat, lng, price, name, store_name, primary_cuisine in marker_data:
                        marker = mu.MarkerWithProps(
                            location=[lat, lng],
                            props = { 'price': price},
                            tooltip = str(primary_cuisine) + '<br>' + store_name + '<br>' + name + '<br>' + str(round(price,2))
                        )
                        marker.add_to(marker_cluster)
                    marker_cluster.add_to(m) 

            else:
                ofo_mean = np.nan
                ofo_median = np.nan
                tabs[i].warning('There is not enough OFO scrape data to show metrics.', icon="⚠️")

            
            if otter_df.shape[0] != 0:
                wm = lambda x: np.average(x, weights=otter_df.loc[x.index, "total_qty_ordered"])
                otter_plot_df = otter_df.groupby(['month']).agg(
                    clean_name=("clean_name", "nunique"),
                    price=("price", "mean"),
                    weighted_price=("price", wm)
                ).reset_index(drop=False)

                otter_max_month = otter_df['month'].max()
                otter_min_month = otter_df['month'].min()

                otter_cumsum_df = otter_df.groupby('price').agg({'total_orders':'sum'}).reset_index(drop=False).sort_values('price', ascending=True)
                otter_cumsum_df['cumsum'] = otter_cumsum_df['total_orders'].cumsum()
                
                recent_updates_by_item_df = otter_df.groupby(['brand_name','item_name']).agg({'month':'max'}).reset_index(drop=False)
                recent_prices_df = pd.merge(otter_df, recent_updates_by_item_df, on=['brand_name','item_name','month'], how='inner')

                recent_prices_df = recent_prices_df.assign(data_source='Otter')
                histogram_df = pd.concat([histogram_df, recent_prices_df[['item_name','price','data_source']]])

                otter_mean = otter_plot_df.price.mean()
                otter_median = otter_plot_df.price.median()
                weighted_otter_mean = np.average(a=otter_df['price'], weights=otter_df['total_orders'])
                otter_mean_last_month = otter_plot_df[otter_plot_df['month']==otter_plot_df['month'].max()]['price'].iloc[0]

                if show_map:
                    map_df = otter_df.groupby(['latitude', 'longitude', 'brand_name', 'item_name']).agg({'price':'mean'}).reset_index(drop=False)

                    marker_cluster = MarkerCluster(icon_create_function=mu.icon_create_function)
                    marker_data = zip(map_df['latitude'], map_df['longitude'], map_df['price'], map_df['item_name'], map_df['brand_name'])
                    for lat, lng, price, name, store_name in marker_data:
                        marker = mu.MarkerWithProps(
                            location=[lat, lng],
                            props = { 'price': price},
                            tooltip= store_name + '<br>' + name + '<br>' + str(round(price,2))
                        )
                        marker.add_to(marker_cluster)
                    marker_cluster.add_to(m)  

            else:
                otter_max_month = np.nan
                otter_min_month = np.nan
                otter_mean = np.nan
                otter_median = np.nan
                weighted_otter_mean = np.nan
                otter_mean_last_month = np.nan
                tabs[i].warning('There is not enough Otter data to show metrics.', icon="⚠️")
            


            summary_ep = tabs[i].expander(label='Summary', expanded=True)

            summary_ep.title('Summary')
            metric_col_ofo_1, metric_col_ofo_2, _, _ = summary_ep.columns(4)
            with metric_col_ofo_1:
                metric_col_ofo_1.metric('OFO Scrape Mean Price', '{}'.format(round(ofo_mean, 2)))
            with metric_col_ofo_2:
                metric_col_ofo_2.metric('OFO Scrape Median Price', '{}'.format(round(ofo_median, 2)))

            metric_col_otter_1, metric_col_otter_2, metric_col_otter_3, metric_col_otter_4 = summary_ep.columns(4)

            with metric_col_otter_1:
                metric_col_otter_1.metric('Otter Mean Price', '{}'.format(round(otter_mean, 2)))
            with metric_col_otter_2:
                metric_col_otter_2.metric('Otter Median Price', '{}'.format(round(otter_median, 2)))
            with metric_col_otter_3:
                metric_col_otter_3.metric('Otter Weighted Mean Price', '{}'.format(round(weighted_otter_mean, 2)))
            with metric_col_otter_4:
                metric_col_otter_4.metric('Otter Last Month Mean Price', '{}'.format(round(otter_mean_last_month, 2)))

            tabs[i].markdown("""---""")

            histogram_df = histogram_df.dropna()
            histogram_data = [
                histogram_df[histogram_df['data_source']=='OFO Scrape']['price'].tolist(),
                histogram_df[histogram_df['data_source']=='Otter']['price'].tolist()    
            ]
            price_hist_ep = tabs[i].expander(label='Price Histogram', expanded=True)
            price_hist_ep.markdown('## Prices Histogram')
            price_hist_ep.write('The following numbers of items were used to calculate these distributions.')
            hist_count_1, hist_count_2, hist_count_3 = price_hist_ep.columns(3)
            with hist_count_1:
                hist_count_1.metric('Overall Item Count', '{}'.format(f"{histogram_df.shape[0]:,}"))
            with hist_count_2:
                hist_count_2.metric('OFO Scrape Item Count', '{}'.format(f"{histogram_df[histogram_df['data_source']=='OFO Scrape'].shape[0]:,}"))
            with hist_count_3:
                hist_count_3.metric('Otter Item Count', '{}'.format(f"{histogram_df[histogram_df['data_source']=='Otter'].shape[0]:,}"))

            fig = ff.create_distplot(histogram_data, ['OFO Scrape', 'Otter'], show_rug=False)
            price_hist_ep.plotly_chart(fig, use_container_width=True)

            price_cumulative_ep = tabs[i].expander(label='Supply / Demand Graph', expanded=True)
            price_cumulative_ep.markdown('### Supply / Demand Graph')
            price_cumulative_ep.markdown('''
                This graph shows the Demand & Supply Curve for the market of the food item(s) you selected. How to interpret the axes:
                * Demand Curve: Given a specific price bucket (X axis) how many % of orders would fetch if we would set the price there.
                * Supply Curve: How many % of items in the market are offered at this price point.
            ''')
            bin_width = price_cumulative_ep.slider(
                label='Choose the size of your price buckets', 
                min_value=0.1,
                max_value=5.0,
                value=1.0,
                step=0.01,
            )

            nbins = math.ceil((otter_df["price"].max() - otter_df["price"].min()) / bin_width)
            otter_cum_df = otter_df[['price', 'total_orders']]
            otter_cum_df['bin'] = pd.cut(otter_cum_df['price'], nbins, include_lowest = True)
            grouped_df = otter_cum_df.groupby('bin').agg({'total_orders':'sum'}).reset_index(drop=False)
            grouped_df['min_price'] = grouped_df['bin'].apply(lambda x: x.left.astype(float))
            grouped_df['max_price'] = grouped_df['bin'].apply(lambda x: float(x.right))
            grouped_df['cumsum'] = grouped_df['total_orders'].cumsum()
            grouped_df['perc_cumsum'] = grouped_df['cumsum'].apply(lambda x: 1 - x/grouped_df['cumsum'].max())
            grouped_df['perc_cumsum_text'] = grouped_df['perc_cumsum'].apply(lambda x: '{}%'.format(round(x*100, 2)))
            grouped_df['bin'] = grouped_df['bin'].astype(str)

            pysqldf = lambda q: sqldf(q, globals())
            otter_ofo_grouped_df = pysqldf('''
                SELECT  
                    a.bin,
                    a.total_orders,
                    CAST(a.min_price AS DOUBLE) AS min_price,
                    a.max_price,
                    a.cumsum,
                    a.perc_cumsum,
                    a.perc_cumsum_text,
                    COUNT(b.item_name) AS ofo_item_count
                FROM grouped_df a
                LEFT JOIN ofo_df b ON (b.price > a.min_price AND b.price <= a.max_price)
                GROUP BY 1,2,3,4,5,6,7
                ORDER BY a.min_price ASC
            ''')
            otter_ofo_grouped_df = otter_ofo_grouped_df.sort_values('min_price', ascending=True)
            otter_ofo_grouped_df['cumsum_ofo'] = otter_ofo_grouped_df['ofo_item_count'].cumsum()
            otter_ofo_grouped_df['perc_cumsum_ofo'] = otter_ofo_grouped_df['cumsum_ofo'].apply(lambda x: x/otter_ofo_grouped_df['cumsum_ofo'].max())
            otter_ofo_grouped_df['perc_cumsum_text_ofo'] = otter_ofo_grouped_df['perc_cumsum_ofo'].apply(lambda x: '{}%'.format(round(x*100, 2)))

            otter_ofo_grouped_df.style.format({'perc_cumsum':"{:.2%}"})
            otter_ofo_grouped_df['price_bins'] = otter_ofo_grouped_df['bin'].astype(str)

            otter_ds_plot_df = otter_ofo_grouped_df[['price_bins', 'perc_cumsum', 'perc_cumsum_text']]
            otter_ds_plot_df['Curve'] = 'Demand Curve'
            ofo_ds_plot_df = otter_ofo_grouped_df[['price_bins', 'perc_cumsum_ofo', 'perc_cumsum_text_ofo']]
            ofo_ds_plot_df.columns = ['price_bins', 'perc_cumsum', 'perc_cumsum_text']
            ofo_ds_plot_df['Curve'] = 'Supply Curve'

            plot_df = pd.concat([otter_ds_plot_df, ofo_ds_plot_df])
            fig = px.line(plot_df, x='price_bins', y='perc_cumsum', color='Curve', markers=True)
            fig.layout.yaxis.title = 'Cumulative Orders % | Cumulative Item Count %'

            price_cumulative_ep.plotly_chart(fig, use_container_width=False)

            # cumsum_subfig = make_subplots(specs=[[{"secondary_y": True}]])
            # # cumsum_fig_bar = px.bar(grouped_df, x='price_bins', y='cumsum')
            # cumsum_fig_line = px.line(otter_ofo_grouped_df, x='price_bins', y='perc_cumsum', text='perc_cumsum_text')
            # cumsum_fig_line.update_traces(line_color='#DD6046', line_width=5)
            # cumsum_fig_line.update_traces(yaxis="y2")
            # cumsum_fig_line.update_traces(
            #     textfont=dict(
            #         size=10,
            #         color="#DD6046",
            #     ),
            #     textposition='top center'
            # )
            # cumsum_fig_ofo_line = px.line(otter_ofo_grouped_df, x='price_bins', y='perc_cumsum_ofo', text='perc_cumsum_text_ofo')
            # cumsum_fig_ofo_line.update_traces(yaxis="y2")
            # cumsum_fig_ofo_line.update_traces(line_color='#696CFA', line_width=5)
            # cumsum_fig_ofo_line.update_traces(
            #     textfont=dict(
            #         size=10,
            #         color="#696CFA",
            #     ),
            #     textposition='top center'
            # )
            # # cumsum_subfig.add_traces(cumsum_fig_bar.data + cumsum_fig_line.data)
            # cumsum_subfig.add_traces(cumsum_fig_ofo_line.data + cumsum_fig_line.data)

            # cumsum_subfig.layout.xaxis.title = 'Price Buckets'
            # # cumsum_subfig.layout.yaxis.title = 'Cumulative Orders'
            # cumsum_subfig.layout.yaxis2.title = 'Cumulative Orders % | Cumulative Item Count %'
            # cumsum_subfig.layout.yaxis2.tickformat = ',.0%'

            # price_cumulative_ep.plotly_chart(cumsum_subfig, use_container_width=False)



            tabs[i].markdown("""---""")

            ofo_historical_ep = tabs[i].expander(label='Historical OFO Prices', expanded=True)
            if ofo_df.shape[0] > 0:
                ofo_historical_ep.markdown('## Historical Prices Ofo Scrapes')
                ofo_historical_ep.write('This chart illustrates the historical price development of OFO scrape means.')

                ofo_plot_df['month'] = pd.to_datetime(ofo_plot_df['month'], format='%Y-%m-%d')

                ofo_subfig = make_subplots(specs=[[{"secondary_y": True}]])
                ofo_fig_bar = px.bar(x=ofo_plot_df['month'], y=ofo_plot_df['clean_name'], color=px.Constant("Item Count"))
                ofo_fig_line = px.line(x=ofo_plot_df['month'], y=ofo_plot_df['price'], color=px.Constant("Mean Price"))
                ofo_fig_line.update_traces(line_color='#DD6046', line_width=5)
                ofo_fig_line.update_traces(yaxis="y2")
                ofo_subfig.add_traces(ofo_fig_bar.data + ofo_fig_line.data)
                ofo_subfig.layout.xaxis.title = 'Month'
                ofo_subfig.layout.yaxis.title = 'Unique Item Count'
                ofo_subfig.layout.yaxis2.title = 'Mean Price'

                ofo_historical_ep.plotly_chart(ofo_subfig, use_container_width=True)

                with tabs[i].expander('OFO Detailed Dataframe', expanded=False):
                    
                    builder = GridOptionsBuilder.from_dataframe(ofo_df[['month', 'primary_cuisine', 'store_name', 'item_name', 'price']])
                    # builder.configure_selection(
                    #     'multiple', 
                    #     use_checkbox=True, 
                    #     groupSelectsChildren=True, 
                    #     groupSelectsFiltered=True
                    # )
                    builder.configure_default_column(
                        groupable=True, 
                        value=True, 
                        enableRowGroup=True, 
                        aggFunc='sum', editable=True
                    )

                    builder.configure_column("month", type=["dateColumnFilter","customDateTimeFormat"], custom_format_string='yyyy-MM-dd')
                    builder.configure_column('item_name', aggFunc='sum')
                    builder.configure_column('price', aggFunc='avg')
                    builder.configure_pagination(paginationAutoPageSize=False, paginationPageSize=100)
                    go = builder.build()

                    #uses the gridOptions dictionary to configure AgGrid behavior.
                    grid_response = AgGrid(
                        data=ofo_df[['month', 'primary_cuisine', 'store_name', 'item_name', 'price']], 
                        gridOptions=go,
                        update_mode=GridUpdateMode.__members__['GRID_CHANGED'],
                        fit_columns_on_grid_load=True,
                        )
                    
                    df = grid_response['data']
                    selected = grid_response['selected_rows']
                    selected_df = pd.DataFrame(selected)

                    # st.dataframe(selected_df)

                    csv = fun.convert_df(ofo_df)
                    st.download_button(
                        "Download Dataframe as CSV",
                        csv,
                        "ofo_scrape_df.csv",
                        "text/csv"
                    )

            otter_historical_ep = tabs[i].expander(label='Historical Otter Prices', expanded=True)
            if otter_df.shape[0] > 0:
                otter_historical_ep.markdown('## Historical Prices Otter')
                otter_historical_ep.write('This chart illustrates the historical price development of Otter means. The weighted mean price is the average price per month, weighted by the quantity the item was ordered.')

                otter_plot_df['month'] = pd.to_datetime(otter_plot_df['month'], format='%Y-%m-%d')
                
                otter_subfig = make_subplots(specs=[[{"secondary_y": True}]])
                otter_fig_bar = px.bar(x=otter_plot_df['month'], y=otter_plot_df['clean_name'], color=px.Constant("Item Count"))
                otter_fig_line = px.line(x=otter_plot_df['month'], y=otter_plot_df['price'], color=px.Constant("Mean Price"))
                otter_fig_line_2 = px.line(x=otter_plot_df['month'], y=otter_plot_df['weighted_price'], color=px.Constant("Weighted Mean Price"))
                otter_fig_line.update_traces(line_color='#DD6046', line_width=5)
                otter_fig_line_2.update_traces(line_color='#ECA610', line_width=5)
                otter_fig_line.update_traces(yaxis="y2")
                otter_fig_line_2.update_traces(yaxis="y2")
                otter_subfig.add_traces(otter_fig_bar.data + otter_fig_line.data + otter_fig_line_2.data)
                otter_subfig.layout.xaxis.title = 'Month'
                otter_subfig.layout.yaxis.title = 'Unique Item Count'
                otter_subfig.layout.yaxis2.title = 'Mean Price'

                otter_historical_ep.plotly_chart(otter_subfig, use_container_width=True)
                
                with tabs[i].expander('Otter Detailed Dataframe', expanded=False):
                    
                    builder = GridOptionsBuilder.from_dataframe(otter_df[['month', 'brand_name', 'item_name', 'price', 'total_orders', 'total_qty_ordered']])
                    # builder.configure_selection(
                    #     'multiple', 
                    #     use_checkbox=True, 
                    #     groupSelectsChildren=True, 
                    #     groupSelectsFiltered=True
                    # )
                    builder.configure_default_column(
                        groupable=True, 
                        value=True, 
                        enableRowGroup=True, 
                        aggFunc='sum', editable=True
                    )

                    builder.configure_column("month", type=["dateColumnFilter","customDateTimeFormat"], custom_format_string='yyyy-MM-dd')
                    builder.configure_column('item_name', aggFunc='sum')
                    builder.configure_column('price', aggFunc='avg')
                    builder.configure_column('total_orders', aggFunc='sum')
                    builder.configure_column('total_qty_ordered', aggFunc='sum')
                    builder.configure_pagination(paginationAutoPageSize=False, paginationPageSize=100)
                    go = builder.build()

                    #uses the gridOptions dictionary to configure AgGrid behavior.
                    grid_response = AgGrid(
                        data=otter_df[['month', 'brand_name', 'item_name', 'price', 'total_orders', 'total_qty_ordered']], 
                        gridOptions=go,
                        update_mode=GridUpdateMode.__members__['GRID_CHANGED'],
                        fit_columns_on_grid_load=True,
                        )
                    
                    df = grid_response['data']
                    selected = grid_response['selected_rows']
                    selected_df = pd.DataFrame(selected)

            tabs[i].markdown("""---""")

            if show_map:
                with tabs[i]:
                        folium_static(m, width=1080)

        except Exception as e:
            tabs[i].write(e)
