import requests
import pandas as pd
import os

from env import get_connection

import prepare as prep


def scrape_data(url='link'):
    """ This function is used to pull data from https://swapi.dev/.
        url - Url for the FIRST page of the data."""
    response = requests.get(url)  # Gets response from url
    data = response.json()  # Create json format response
    df = pd.DataFrame(data['results'])  # Create df from results
    page = data.copy()  # Create extra page to 'flip' through
    while page['next'] is None:  # Create a while loop that will continue until last page
        page = requests.get(page['next']).json()
        df = pd.concat([df, pd.DataFrame(page['results'])], ignore_index=True)  # Adds new data to the dataframe
    return df  # Return df


def sql_query(db='None', query='None'):
    """ This is a function to easily and quickly create a SQL query in python.
       db - String name of database which to access
       query - SQL query to run."""
    db_url = get_connection(db)
    df = pd.read_sql(query, db_url)
    return df  # Returns df from the query that was input


def get_opds(dt=True, ind=True, sor=True):
    filename = 'opsd_germany_daily.csv'
    if os.path.isfile(filename):
        df = pd.read_csv(filename)
        df = prep.create_index(df, 'date', datetime=dt, index=ind, sort=sor)  # Re-index df with datetime object
        return df  # Returns local file if there is one
    else:
        df = pd.read_csv('https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv')
        df.columns = df.columns.str.lower()  # Lower case column names
        df.columns = df.columns.str.replace('+', '_')  # Replace '+' with '_'
        df = prep.create_index(df, 'date', datetime=dt, index=ind, sort=sor)  # Re-index df with datetime object
        df = df.fillna(0)  # Fill nulls with 0
        df.wind_solar = df.wind + df.solar  # Recreate wind_solar column
        df.to_csv(filename)  # Save to .csv file
        return df  # Return df


def get_items(dt=True, ind=True, sor=True):
    filename = 'items.csv'
    if os.path.isfile(filename):
        df = pd.read_csv(filename)  # Returns local file if there is one
        df = prep.create_index(df, 'sale_date', datetime=dt, index=ind, sort=sor)  # Re-index df with datetime object
        return df  # Returns local file if there is one
    else:
        query = ''' SELECT sale_date, sale_amount, item_brand, item_name, item_price, 
                    store_address, store_zipcode, store_city, store_state  
                    FROM sales AS s
                    LEFT JOIN items as i USING(item_id)
                    LEFT JOIN stores as st USING(store_id)'''
        df = sql_query('tsa_item_demand', query)
        df = prep.create_index(df, 'sale_date', datetime=dt, index=ind, sort=sor)  # Re-index df with datetime object
        df.to_csv(filename)  # Save to .csv file
        return df  # Return df
