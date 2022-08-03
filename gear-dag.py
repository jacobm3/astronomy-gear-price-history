import boto3
import json
import os
import pprint
import re
import requests
import subprocess

from datetime import datetime, timedelta

from airflow.decorators import dag, task # DAG and task decorators for interfacing with the TaskFlow API


@dag(
    # This defines how often your DAG will run, or the schedule by which your DAG runs. In this case, this DAG
    # will run daily
    schedule_interval="0 */6 * * *",
    # This DAG is set to run for the first time on January 1, 2021. Best practice is to use a static
    # start_date. Subsequent DAG runs are instantiated based on scheduler_interval
    start_date=datetime(2021, 1, 1),
    # When catchup=False, your DAG will only run for the latest schedule_interval. In this case, this means
    # that tasks will not be run between January 1, 2021 and 30 mins ago. When turned on, this DAG's first
    # run will be for the next 30 mins, per the schedule_interval
    catchup=False,
    default_args={
        "retries": 2, # If a task fails, it will retry 2 times.
    },
    tags=['example']) # If set, this tag is shown in the DAG view of the Airflow UI
def gear():
    """
    ### Basic ETL Dag
    This is a simple ETL data pipeline example that demonstrates the use of
    the TaskFlow API using three simple tasks for extract, transform, and load.
    For more information on Airflow's TaskFlow API, reference documentation here:
    https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html
    """

    @task()
    def getUrlList():
        """
        Retrieves list of SKUs and URLs to scan for price data.
        """
        client = boto3.client('dynamodb')
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('astro-gear-product-price-url-v2')
        response = table.scan()
        data = response['Items']
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            data.extend(response['Items'])

        return data

    @task(multiple_outputs=True) # multiple_outputs=True unrolls dictionaries into separate XCom values
    def scrapePrice(url_records: dict):
        """
        Scrapes a web page looking for the product price. Trial and error getting this to work on multiple sites.
        """
        url = url_records['URL']
        result = subprocess.run(['lynx','-dump','-accept_all_cookies', url], capture_output=True, text=True)

        # Exclude 'original price' lines
        re_original = re.compile(r'original', re.I)
        filtered_out = ''
        for ln in result.stdout.split('\n'):
            m = re_original.search(str(ln))
            if not m:
                filtered_out = filtered_out + ln

        price = None
        if url_records['SELLER'] == 'optcorp.com':
            re_price = re.compile(r'''(Price|SKU).{1,300}\$.*?
                                      (\d{1,3}(?:[,]\d{3})*(?:[.]\d{2})?)
                                      ''', flags=re.I|re.X)
            m = re_price.search(filtered_out)
            if m:
                price = m.group(2)

        else:
            re_price = re.compile(r'''(current)*\s(Price|SKU).{0,20}\$\s*
                                      (\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2}))
                                      ''', flags=re.I|re.X)
            m = re_price.search(filtered_out)
            if m:
                price = m.group(3)

        if price:
            price = price.replace(',','')
            price = "{:.2f}".format(float(price))
        print('Price: %s' % price)
        url_records['PRICE'] = price
        url_records['DATE'] = datetime.now().isoformat()
        return url_records

    @task()
    def savePrices(price_list):
        """
        Save the resulting price data to the astro-gear-price-history table.
        """

        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('astro-gear-price-history')
        print('Price List:')
        for x in price_list:
            result = x
            print('table.put_item(Item=result)')
            print('result: %s' % result)
            response = table.put_item(Item=result)

    url_records = getUrlList()
    price_list = scrapePrice.expand(url_records=url_records)
    savePrices(price_list)

gear = gear()
