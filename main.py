import sys
sys.path.append('/home/maximilian.hofmann/tools/brand_science_pricing')
sys.path.append('/home/maximilian.hofmann/ff_utils/src')

import constants as const
import functions as fun

import numpy as np
import pandas as pd
import streamlit as st
from streamlit_folium import folium_static
import folium
from folium.plugins import MarkerCluster
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.figure_factory as ff

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

#####################
### Text Block
#####################
st.title('Pricing Tool | Brand Science')
st.markdown('This tool gives a hollistic overview of setting an initial price for a menu item in a given geography. We are combining supply (OFO scrape) and demand (Otter orders) data in order to calculate an effective price.')
st.markdown('More details on the methodology can be found [here](https://docs.google.com/document/d/1VODjLh1_7F_2lE0IhuTGQRVaga546Vy7Gy4WLNbDwVE/edit#heading=h.8n2n4oy69nrt).')
st.markdown('We need your feedback! Please use [this form](https://forms.gle/mZmqGKw5Q1bTMa97A) to submit your feature requests, bug reports and wishes.')
st.markdown('If you have any questions, please reach out to @maximilian.hofmann.')

#####################
### User Inputs
#####################  
with st.form("query_form"):
    st.subheader("Please select your inputs:")
    city, country_code, radius = st.columns(3)
    with city:
        city_input = st.text_input('City', 'London')

    with country_code:
        country_input = st.selectbox('Country', options=const.country_codes, index=0)

    with radius:
        radius_input = st.number_input('Radius (in km)', min_value=0, value=10)
        radius_input = radius_input * 1000

    search_option, search_string = st.columns([2, 8])
    with search_option:
        search_method = st.radio('String Search Method', ('Contains', 'Equals'))
    with search_string:
        search_string_inputs = st.text_input('Please type in the items you are looking for. If multiple, please separate by comma.')

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

    run_query_button = st.form_submit_button("Run Query")

if run_query_button:

    lat, lng = fun.get_lat_lng_from_str(city_input)

    # LOG ACTION
    query_args = {
        'testing':True,
        'country_code':country_input,
        'city': city_input,
        'lat': lat,
        'lng': lng,
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

        m = folium.Map(location=[lat, lng], zoom_start=10)
        folium.Circle([lat, lng], radius=radius_input).add_to(m)

        tabs[i].title('Summary')

        try:

            ### CALCULATIONS
            ### Supply
            ofo_df = fun.load_ofo_data(
                country_input, 
                lat, 
                lng, 
                fun.get_search_term_query(search_string_input, search_method), 
                fun.get_exclude_term_query(exclude_string_input), 
                radius_input, 
                fun.get_exclude_primary_cuisines_query(exclude_primary_cuisine),
                fun.get_include_primary_cuisines_query(include_primary_cuisine),
                fun.get_exclude_store_names_query(exclude_store_names)
                ) 

            histogram_df = pd.DataFrame()

            if ofo_df.shape[0] > 0:
                ofo_mean = ofo_df.price.mean()
                ofo_median = ofo_df.price.median()

                ofo_histogram = px.histogram(ofo_df, x='price')
                ofo_histogram_df = ofo_df[['item_name', 'price']]
                ofo_histogram_df = ofo_histogram_df.assign(data_source='OFO Scrape')

                histogram_df = pd.concat([histogram_df, ofo_histogram_df])

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

            ### Demand
            otter_df = fun.load_otter_data(
                country_input, 
                lat, 
                lng, 
                fun.get_search_term_query(search_string_input, search_method), 
                fun.get_exclude_term_query(exclude_string_input), 
                radius_input,
                fun.get_exclude_brand_names_query(exclude_store_names)
            )

            if otter_df.shape[0] != 0:
                wm = lambda x: np.average(x, weights=otter_df.loc[x.index, "total_qty_ordered"])
                otter_plot_df = otter_df.groupby(['month']).agg(
                    clean_name=("clean_name", "nunique"),
                    price=("price", "mean"),
                    weighted_price=("price", wm)
                ).reset_index(drop=False)
                
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
                otter_mean = np.nan
                otter_median = np.nan
                weighted_otter_mean = np.nan
                otter_mean_last_month = np.nan
                tabs[i].warning('There is not enough Otter data to show metrics.', icon="⚠️")

            metric_col_ofo_1, metric_col_ofo_2, _, _ = tabs[i].columns(4)
            with metric_col_ofo_1:
                metric_col_ofo_1.metric('OFO Scrape Mean Price', '{}'.format(round(ofo_mean, 2)))
            with metric_col_ofo_2:
                metric_col_ofo_2.metric('OFO Scrape Median Price', '{}'.format(round(ofo_median, 2)))

            metric_col_otter_1, metric_col_otter_2, metric_col_otter_3, metric_col_otter_4 = tabs[i].columns(4)

            with metric_col_otter_1:
                metric_col_otter_1.metric('Otter Mean Price', '{}'.format(round(otter_mean, 2)))
            with metric_col_otter_2:
                metric_col_otter_2.metric('Otter Median Price', '{}'.format(round(otter_median, 2)))
            with metric_col_otter_3:
                metric_col_otter_3.metric('Otter Weighted Mean Price', '{}'.format(round(weighted_otter_mean, 2)))
            with metric_col_otter_4:
                metric_col_otter_4.metric('Otter Last Month Mean Price', '{}'.format(round(otter_mean_last_month, 2)))

            # histogram_chart = px.histogram(
            #     histogram_df, 
            #     x='price', 
            #     color='data_source', 
            #     barmode='overlay', 
            #     histnorm='density',
            #     title=
            #     )
            # tabs[i].plotly_chart(histogram_chart, use_container_width=True)

            histogram_df = histogram_df.dropna()
            histogram_data = [
                histogram_df[histogram_df['data_source']=='OFO Scrape']['price'].tolist(),
                histogram_df[histogram_df['data_source']=='Otter']['price'].tolist()    
            ]

            tabs[i].markdown('## Prices Histogram')
            tabs[i].write('The following numbers of items were used to calculate these distributions.')
            hist_count_1, hist_count_2, hist_count_3 = tabs[i].columns(3)
            with hist_count_1:
                hist_count_1.metric('Overall Item Count', '{}'.format(f"{histogram_df.shape[0]:,}"))
            with hist_count_2:
                hist_count_2.metric('OFO Scrape Item Count', '{}'.format(f"{histogram_df[histogram_df['data_source']=='OFO Scrape'].shape[0]:,}"))
            with hist_count_3:
                hist_count_3.metric('Otter Item Count', '{}'.format(f"{histogram_df[histogram_df['data_source']=='Otter'].shape[0]:,}"))

            fig = ff.create_distplot(histogram_data, ['OFO Scrape', 'Otter'], show_rug=False)
            tabs[i].plotly_chart(fig, use_container_width=True)

            if otter_df.shape[0] > 0:
                tabs[i].markdown('## Historical Prices Otter')
                tabs[i].write('This chart illustrates the historical price development of Otter means. The weighted mean price is the average price per month, weighted by the quantity the item was ordered.')

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


                tabs[i].plotly_chart(otter_subfig, use_container_width=True)


            if show_map:
                with tabs[i]:
                    folium_static(m, width=725)

            if ofo_df.shape[0] > 0:
                tabs[i].subheader('OFO Scrape Data')
                # tabs[i].write(
                #     'The OFO mean price in {city}, {country_code} in a radius of {radius} meters around the city center is {ofo_mean}. {item_count} items were used for the calculation.'.format(
                #         city=city_input,
                #         country_code = country_input,
                #         radius = radius_input,
                #         ofo_mean = round(ofo_mean,2),
                #         item_count = ofo_df.shape[0]
                #     )
                # )

                # tabs[i].plotly_chart(ofo_histogram, use_container_width=True)

                tabs[i].write(ofo_df[['store_name', 'primary_cuisine', 'item_name', 'price']].drop_duplicates())

            if otter_df.shape[0] > 0:

                tabs[i].subheader('Otter Data')
                tabs[i].write('The Otter data mean price in {city}, {country_code} in a radius of {radius} meters around the city center is {demand_mean}. \nWeighted by order volume the mean price would be {weighted_demand_mean}. {item_count} items were used for the calculation.'.format(
                    city=city_input,
                    country_code = country_input,
                    radius = radius_input,
                    demand_mean = round(otter_mean,2),
                    weighted_demand_mean = round(weighted_otter_mean,2),
                    item_count = otter_df.shape[0]
                ))

                otter_plot_df['month'] = pd.to_datetime(otter_plot_df['month'], format='%Y-%m-%d')


                otter_subfig = make_subplots(specs=[[{"secondary_y": True}]])
                otter_fig_bar = px.bar(x=otter_plot_df['month'], y=otter_plot_df['clean_name'])
                otter_fig_line = px.line(x=otter_plot_df['month'], y=otter_plot_df['price'])
                otter_fig_line.update_traces(line_color='#DD6046', line_width=5)
                otter_fig_line.update_traces(yaxis="y2")
                otter_subfig.add_traces(otter_fig_bar.data + otter_fig_line.data)
                otter_subfig.layout.xaxis.title = 'Month'
                otter_subfig.layout.yaxis.title = 'Unique Item Count'
                otter_subfig.layout.yaxis2.title = 'Mean Price'


                tabs[i].dataframe(otter_plot_df)

                st.plotly_chart(otter_subfig)

                tabs[i].dataframe(otter_df[['brand_name', 'item_name', 'clean_name','month', 'price', 'total_orders']].drop_duplicates())

        except Exception as e:
            tabs[i].write(e)
