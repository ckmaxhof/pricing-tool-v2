import sys
sys.path.append('/home/maximilian.hofmann/tools/brand_science_pricing')
sys.path.append('/home/maximilian.hofmann/ff_utils/src')

import config

from google_utils import BigQuery
from string_utils import *

from datetime import datetime
import geopy
import json
import numpy as np
import pandas as pd
import streamlit as st
import socket

if socket.gethostname() == 'CSS-C02GK2KGQ6LT':
    svc_account_file = '/Users/maximilian.hofmann/projects/_secrets/brand_science_svc_account.json'
else:
    svc_account_file='/home/maximilian.hofmann/_secrets/brand_science_svc_account.json'

bq = BigQuery(svc_account_file=svc_account_file)

#################################
### QUERY FUNCTIONS
#################################

@st.cache(max_entries=10, ttl=600)
def load_ofo_data(country_codes_filter, geo_filters, string_match, string_exclude, exclude_primary_cuisine,include_primary_cuisine,exclude_store_names):
        
    ofo_sql_gc = '''
        SELECT month, item_name, clean_name, price, latitude, longitude, store_name, primary_cuisine
        FROM `css-operations.brand_science.pricing_ofo_scrape_data`
        WHERE 1=1
            {country_codes_filter}
            {geo_filters}
            {string_match}
            {string_exclude}
            {exclude_primary_cuisine}
            {include_primary_cuisine}
            {exclude_store_names}
    '''.format(
        country_codes_filter=country_codes_filter, 
        geo_filters=geo_filters,
        string_match=string_match, 
        string_exclude=string_exclude, 
        exclude_primary_cuisine=exclude_primary_cuisine, 
        include_primary_cuisine=include_primary_cuisine,
        exclude_store_names=exclude_store_names
        )

    return bq.run_query(ofo_sql_gc)

@st.cache(max_entries=10, ttl=600)
def load_otter_data(country_codes_filter, geo_filters, string_match, string_exclude, exclude_brand_names):
    
    otter_sql_gc = '''
        SELECT brand_name, item_name, clean_name, CAST(month AS DATE) AS month, price, total_orders, total_qty_ordered, latitude, longitude
        FROM `css-operations.brand_science.pricing_otter_data`
        WHERE 1=1
            {country_codes_filter}
            {geo_filters}
            {string_match}
            {string_exclude}
            {exclude_brand_names}
    '''.format(
        country_codes_filter=country_codes_filter, 
        geo_filters=geo_filters,
        string_match=string_match, 
        string_exclude=string_exclude, 
        exclude_brand_names=exclude_brand_names
    )

    return bq.run_query(otter_sql_gc)

@st.cache
def load_primary_cuisine():
    q = 'SELECT DISTINCT primary_cuisine FROM `css-operations.brand_science.pricing_ofo_scrape_data`'
    df = bq.run_query(q)
    return np.unique([x.lower() for x in df['primary_cuisine'].tolist() if x != None])

@st.cache
def load_cities():
    return pd.read_csv('worldcities.csv').sort_values(['iso2'])

@st.cache
def load_store_names():
    q = 'SELECT DISTINCT store_name FROM `css-operations.brand_science.pricing_ofo_scrape_data`'
    df = bq.run_query(q)
    return np.unique([x.lower() for x in df['store_name'].tolist() if x != None])

#################################
### QUERY ARGS FUNCTIONS
#################################

def get_country_codes_filter(country_codes):
    
    if country_codes != '' and len(country_codes) > 0:
            return '''AND country_code IN ("{}")'''.format('","'.join(country_codes))
    else:
        return ''

def get_st_distance_filters(lats, lngs, radius):
    s = 'AND ('
    for i, (lat, lng) in enumerate(zip(lats, lngs)):
        
        if i == len(lats)-1:
            s += '''
                ST_DISTANCE(
                    ST_GEOGPOINT(longitude, latitude),
                    ST_GEOGPOINT({lng},   {lat})
                ) <= {radius}

            '''.format(lat=lat, lng=lng, radius=radius)
        
        else:
            s += '''
                ST_DISTANCE(
                    ST_GEOGPOINT(longitude, latitude),
                    ST_GEOGPOINT({lng},   {lat})
                ) <= {radius} OR

            '''.format(lat=lat, lng=lng, radius=radius)

    return s + ')'

def get_search_term_query(search_term, search_method='Contains'):
    l = [string_to_clean_name(x).strip() for x in search_term.split(' ')]
    
    string_match = ''
    if search_method=='Contains':
        for st in l:
            string_match += '''
                AND (
                    clean_name LIKE '%%{}%%'
                    )
            '''.format(st)
    elif search_method=='Equals':
        string_match = 'AND clean_name = "{}"'.format(' '.join(l))

    return string_match

def get_exclude_term_query(search_term):
    l = [string_to_clean_name(x).strip() for x in search_term.split(',')]

    if len(l)>0 and search_term != '':
        string_match = ''
        for st in l:
            string_match += '''
                AND NOT (
                    clean_name LIKE '%%{}%%'
                    )
                AND NOT (
                    item_name LIKE '%%{}%%'
                    )
            '''.format(st, st)
        print(string_match)
        return string_match
    else:
        return ''

def get_exclude_primary_cuisines_query(primary_cuisines):
    
    if primary_cuisines != '' and len(primary_cuisines) > 0:
            return '''AND NOT LOWER(primary_cuisine) IN ("{}")'''.format('","'.join(primary_cuisines))
    else:
        return ''

def get_exclude_store_names_query(store_names):
    
    if store_names != '':
        l = [x for x in store_names.split(',')]
        string_match = ''
        for st in l:
            string_match += '''
                AND NOT (
                    LOWER(store_name) LIKE '%%{}%%'
                    )
            '''.format(st)
            
        return string_match
    else:
        return ''

def get_exclude_brand_names_query(brand_names):
    
    if brand_names != '':
        # l = [string_to_clean_name(x).strip() for x in store_names.split(',')]
        l = [x for x in brand_names.split(',')]
        string_match = ''
        for st in l:
            string_match += '''
                AND NOT (
                    LOWER(brand_name) LIKE '%%{}%%'
                    )
            '''.format(st)
            
        return string_match
    else:
        return ''

def get_include_primary_cuisines_query(primary_cuisines):
    
    if len(primary_cuisines)>1 and primary_cuisines != '':
            return '''AND LOWER(primary_cuisine) IN ("{}")'''.format('","'.join(primary_cuisines))
    else:
        return ''

def get_lat_lng_from_str(search_string):
    geo_locator = geopy.Nominatim(user_agent='1234')
    res = geo_locator.geocode(search_string)
    lat = res.latitude
    lng = res.longitude
    
    return lat, lng

#################################
### TRACKING FUNCTIONS
#################################

def log_user_actions(d, action_type, json_args=None, table=config.user_tracking_table):
    d['action_type'] = action_type
    d['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if json_args:
        d['args'] = json.dumps(json_args)

    errors=bq.client.insert_rows_json(table, [d])

    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))

def set_session_state_0():
    if 's' not in st.session_state:
        st.session_state.s = 0
    else:
        st.session_state.s = 0

@st.cache
def convert_df(df):
   return df.to_csv().encode('utf-8')
