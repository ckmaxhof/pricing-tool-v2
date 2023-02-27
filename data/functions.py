import sys
sys.path.append('/home/maximilian.hofmann/ff_utils/src')

import geo_utils
from string_utils import *
from google_utils import BigQuery

from datetime import datetime
import pandas as pd
import numpy as np
import unicodedata
from ds.utilities.io import ds_trino

import const

oauth_file = '/home/maximilian.hofmann/_secrets/gc_oauth_brand_science.json'
svc_account_file = '/home/maximilian.hofmann/_secrets/brand_science_svc_account.json'

def get_clean_item_name_column(df, source_col = 'item_name'):
    
    print('Start data cleaning...')
    
    df['clean_name'] = df[source_col].apply(lambda x: unicodedata.normalize("NFKC", x))
    df['clean_name'] = df['clean_name'].apply(clean_item_string_from_count)
    df['clean_name'] = df['clean_name'].apply(clean_item_string_from_drink)
    df['clean_name'] = df['clean_name'].apply(clean_item_string_from_weight)

    df['item_count'] = df[source_col].apply(get_item_count_from_string)
    df['drink_qty'] = df[source_col].apply(get_drink_qty_from_string)
    df['weight'] = df[source_col].apply(get_weight_from_string)
    
    df['clean_name'] = df['clean_name'].apply(remove_punctuation_digits_from_string)
    df['clean_name'] = df['clean_name'].apply(remove_stopwords_new_new)
    df['clean_name'] = df['clean_name'].apply(stem_sentence)
    
    print('Data cleaning finished!')
    
    return df


def demand_clean_data(df):
    print('Start data cleaning...')
    
    df['clean_name'] = df['item_name'].swifter.apply(lambda x: unicodedata.normalize("NFKC", x))
    df['clean_name'] = df['clean_name'].swifter.apply(clean_item_string_from_count)
    df['clean_name'] = df['clean_name'].swifter.apply(clean_item_string_from_drink)
    df['clean_name'] = df['clean_name'].swifter.apply(clean_item_string_from_weight)

    df['item_count'] = df['item_name'].swifter.apply(get_item_count_from_string)
    df['drink_qty'] = df['item_name'].swifter.apply(get_drink_qty_from_string)
    df['weight'] = df['item_name'].swifter.apply(get_weight_from_string)
    
    df['clean_name'] = df['clean_name'].swifter.apply(remove_punctuation_digits_from_string)
    df['clean_name'] = df['clean_name'].swifter.apply(remove_stopwords)
    df['clean_name'] = df['clean_name'].swifter.apply(stem_sentence)
    
    df = df.swifter.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    
    df['month'] = pd.to_datetime(df['month'])
    
    df['_loaded_at'] = pd.to_datetime(datetime.now())
    
    df = df[df['mean_price']!='NaN']
    
    print('Data cleaning finished!')

def process_ofo_data(t, full_table_name, clustering_fields=['country_code','month']):
    
    print('Beginn process data...')
    
    t = get_clean_item_name_column(t)
    t = geo_utils.get_country_codes_from_latlng_df(t)
    
    t['categories'] = t['categories'].apply(lambda x: ','.join(x).lower() if isinstance(x, list) else None)
    t['month'] = pd.to_datetime(t['month'])
    t['_loaded_at'] = pd.to_datetime(datetime.now())
    
    t = t[['item_name', 'clean_name', 'price', 'currency_code',
           'item_count', 'drink_qty', 'weight', 'month', 'store_name',
           'external_store_id', 'categories', 'primary_cuisine', 'latitude',
           'longitude', 'city', 'country_code', 'service_slug', 'source', '_loaded_at'
          ]].reset_index(drop=True)
    
    bg = BigQuery(svc_account_file='/home/maximilian.hofmann/_secrets/brand_science_svc_account.json')
    
    load_job = bg.upload_to_table_from_df(
        df=t, 
        tbl_id=full_table_name,
        clustering_fields=clustering_fields,
        schema=const.pricing_ofo_scrape_data_schema,
        how='WRITE_APPEND'
    )
    
    print('Done!')
    
    return load_job

def process_ofo_data_date_range(sql_template, date_range, collection, service_slug, full_table_name, clustering_fields): 
    
    sql = sql_template.format(begin_date=date_range[0], end_date=date_range[1], collection=collection, service_slug=service_slug)    
    data = ds_trino.fetch_data(sql)
    
    if data.shape[0] > 0:
    
        data = get_clean_item_name_column(data, 'item_name')
        data = geo_utils.get_country_codes_from_latlng_df(data)

        data['categories'] = data['categories'].apply(lambda x: ','.join(x).lower() if isinstance(x, list) else None)
        data['month'] = pd.to_datetime(data['month'])
        data['_loaded_at'] = pd.to_datetime(datetime.now())

        data['unique_key'] = data['month'].astype(str) + data['external_store_id'] + data['item_id']

        data = data[[
            'month', 'item_id', 'item_name', 'clean_name', 'price', 'currency_code',
            'item_count', 'drink_qty', 'weight', 'store_name',
            'external_store_id', 'categories', 'primary_cuisine', 'latitude',
            'longitude', 'city', 'country_code', 'service_slug', 'source', '_loaded_at', 'unique_key'
        ]].reset_index(drop=True)

        data['latitude'] = data['latitude'].astype(np.float64)
        data['longitude'] = data['longitude'].astype(np.float64)

        bg = BigQuery(svc_account_file='/home/maximilian.hofmann/_secrets/brand_science_svc_account.json')

        return bg.upload_to_table_from_df(
            df=data, 
            tbl_id=full_table_name,
            clustering_fields=clustering_fields,
            schema=const.pricing_ofo_scrape_data_schema,
            how='WRITE_APPEND'
        )
    
def process_ofo_data_date_range_iceberg(sql_template, date_range, super_region, service_slug, full_table_name, clustering_fields): 
    
    sql = sql_template.format(begin_date=date_range[0], end_date=date_range[1], super_region=super_region, service_slug=service_slug)    
    data = ds_trino.fetch_data(sql)
    
    if data.shape[0] > 0:
    
        data = get_clean_item_name_column(data, 'item_name')
        data = geo_utils.get_country_codes_from_latlng_df(data)

        data['categories'] = data['categories'].apply(lambda x: ','.join(x).lower() if isinstance(x, list) else None)
        data['month'] = pd.to_datetime(data['month'])
        data['_loaded_at'] = pd.to_datetime(datetime.now())

        data['unique_key'] = data['month'].astype(str) + data['external_store_id'] + data['item_id']

        data = data[[
            'month', 'item_id', 'item_name', 'clean_name', 'price', 'currency_code',
            'item_count', 'drink_qty', 'weight', 'store_name',
            'external_store_id', 'categories', 'primary_cuisine', 'latitude',
            'longitude', 'city', 'country_code', 'service_slug', 'source', '_loaded_at', 'unique_key'
        ]].reset_index(drop=True)

        data['latitude'] = data['latitude'].astype(np.float64)
        data['longitude'] = data['longitude'].astype(np.float64)

        bg = BigQuery(svc_account_file='/home/maximilian.hofmann/_secrets/brand_science_svc_account.json')

        return bg.upload_to_table_from_df(
            df=data, 
            tbl_id=full_table_name,
            clustering_fields=clustering_fields,
            schema=const.pricing_ofo_scrape_data_schema,
            how='WRITE_APPEND'
        )


def process_otter_data_date_range(sql_template, date_range, country_code, full_table_name, clustering_fields):
    
    sql = sql_template.format(begin_date=date_range[0], end_date=date_range[1], country_code=country_code)
    data = ds_trino.fetch_data(sql)
    
    data = get_clean_item_name_column(data, 'normalized_item_name')
    
    data['month'] = pd.to_datetime(data['month'])
    data['_loaded_at'] = pd.to_datetime(datetime.now())
    data = data[data['price']!='NaN']
    
    data['unique_key'] = data['month'].astype(str) + data['store_id'] + data['item_id']
    
    data = data[['month', 'brand_name', 'is_ff_brand', 'facility_id', 'latitude',
       'longitude', 'timezone', 'country_code', 'store_id', 'item_id',
       'item_name', 'normalized_item_name', 'clean_name', 'currency_code', 'number_of_days',
       'total_qty_ordered', 'total_orders', 'quantity_per_day',
       'quantity_per_order', 'price', 'item_count', 'drink_qty',
       'weight', '_loaded_at', 'unique_key']] 
    
    data['latitude'] = data['latitude'].astype(np.float64)
    data['longitude'] = data['longitude'].astype(np.float64)
    
    bg = BigQuery(svc_account_file='/home/maximilian.hofmann/_secrets/brand_science_svc_account.json')
    
    return bg.upload_to_table_from_df(
        df=data, 
        tbl_id=full_table_name,
        clustering_fields=clustering_fields,
        schema=const.pricing_otter_data_schema,
        how='WRITE_APPEND'
    )

def create_tbl(stag_table, prod_table):
    bg = BigQuery(svc_account_file='/home/maximilian.hofmann/_secrets/brand_science_svc_account.json')
    
    create_tbl_sql = '''
        CREATE TABLE IF NOT EXISTS {} AS SELECT * FROM {} LIMIT 1
    '''.format(prod_table, stag_table)
    
    return bg.run_query(create_tbl_sql)

def upsert_tbl_otter_data(stag_table, prod_table):
    bg = BigQuery(svc_account_file='/home/maximilian.hofmann/_secrets/brand_science_svc_account.json')
    
    upsert_sql = '''
        MERGE {} prod
        USING {} stag
        ON prod.unique_key = stag.unique_key

        WHEN NOT MATCHED THEN
            INSERT (
                month, 
                brand_name,
                is_ff_brand,
                facility_id,
                latitude,
                longitude,
                timezone,
                country_code,
                store_id,
                item_id,
                item_name,
                normalized_item_name,
                clean_name,
                currency_code,
                number_of_days,
                total_qty_ordered,
                total_orders,
                quantity_per_day,
                quantity_per_order,
                price,item_count,
                drink_qty,
                weight,
                _loaded_at,
                unique_key
            )
            VALUES(
                month,
                brand_name,
                is_ff_brand,
                facility_id,
                latitude,
                longitude,
                timezone,
                country_code,
                store_id,
                item_id,
                item_name,
                normalized_item_name,
                clean_name,
                currency_code,
                number_of_days,
                total_qty_ordered,
                total_orders,
                quantity_per_day,
                quantity_per_order,
                price,item_count,
                drink_qty,
                weight,
                _loaded_at,
                unique_key
            )
    '''.format(prod_table, stag_table)
    
    return bg.run_query(upsert_sql)

def upsert_tbl_ofo_scrape_data(stag_table, prod_table):
    bg = BigQuery(svc_account_file='/home/maximilian.hofmann/_secrets/brand_science_svc_account.json')
    
    upsert_sql = '''
        MERGE {} prod
        USING {} stag
        ON prod.unique_key = stag.unique_key

        WHEN NOT MATCHED THEN
            INSERT(
                month, 
                item_id,
                item_name,
                clean_name,
                price,
                currency_code,
                item_count,
                drink_qty,
                weight,
                store_name,
                external_store_id,
                categories,
                primary_cuisine,
                latitude,
                longitude, 
                city,
                country_code,
                service_slug,
                source,
                _loaded_at,
                unique_key
            )
            VALUES(
                month, 
                item_id,
                item_name,
                clean_name,
                price,
                currency_code,
                item_count,
                drink_qty,
                weight,
                store_name,
                external_store_id,
                categories,
                primary_cuisine,
                latitude,
                longitude, 
                city,
                country_code,
                service_slug,
                source,
                _loaded_at,
                unique_key
            )
    '''.format(prod_table, stag_table)
    
    return bg.run_query(upsert_sql)