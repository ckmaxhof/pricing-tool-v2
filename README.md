# Pricing Tool V2

## Set up your enviroment
1. Execute `pip install -r requirements.txt` to install all required packages
2. Exchange the name `maximilian.hofmann` by your name in all files 

## Data Backend
### Updating the Data Availability Sheet
The [Data Availability Sheet](https://www.google.com/url?sa=t&rct=j&esrc=s&source=appssearch&uact=8&cd=0&cad=rja&q=&sig2=rwRzhhTlI2SMkT3U4qH72A&ved=0ahUKEwjDs8Taz8H7AhUNzqcKHU2MByk4ABABKAAwAA&url=https://drive.google.com/a/cloudkitchens.com/open%3Fid%3D1gxB_60JSDvb2nVObVfSFB66qfWRIFyyNoAcEh-AvmcU&usg=AOvVaw1ttcQb1PydW-dgzI0sQXft) shows how much data is available for which countries. Stakeholders should be able to see on which data they're basing their analysis on. 
In order to automate the refreshing of this sheet, please run & schedule the file `_data_check.pipeline` in the subdirectory `data`. This will execute the notebook `_data_check.ipynb`.
### Deduping data
In case something in the ETL goes wrong and we find duplicate data in the production table, you can execute the notebook `_dedupe_data.ipynb` to resolve this.
### Monthly Executions
Data cadence in the tool is currently monthly, which means that the scripts need to be executed on a monthly basis. This is the case for both Otter and OFO Scrape data. Ideally you execute scripts at the end of the month, to make sure that all data is available. 
#### Otter Data 
Otter data can be executed via the scripts `get_otter_data_{COUNTRY_CODE}.ipynb`. Around cell 10, please make sure that you set the correct `begin_date` and `end_date` as well as double check the `country_code` variable. The you can simply execute the ETL either in your jupyterhub instance or by selecting `Run as Pipeline`.
#### OFO Scrape Data
Please open the notebook `get_ofo_scrape_data_iceberg.ipynb`. Scroll down and select the correct `begin_date`, `end_date`, `super_region` and `service_slug`. If you don't know which service slugs are available in which country, you can uncomment the query at the beginning and execute it to see which data are available.

```
# sql = '''
# SELECT DISTINCT 
#     CAST(DATE_TRUNC('MONTH', last_seen) AS DATE) AS month,
#     super_region,
#     country_code,
#     service_slug,
#     COUNT(*)
# FROM iceberg.marketintel.storefront_latest_view
# GROUP BY 1,2,3,4
# ORDER BY 2,1,3,4
# '''
```

After you set the correct variables you can also hit `Run as Pipeline`.

* Attention! Monitor the execution closely. It might fail because the instances run out of memory. In this case you would need to decrease the amount of days between begin_date and end_date.