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
    if db == 'None':  # Will alert you that no database was specified
        print('Database not specified.')
    elif query == 'None':  # Will alert you that no query was specified
        print('No query!')
    else:
        db_url = get_connection(db)
        df = pd.read_sql(query, db_url)
        return df  # Returns df from the query that was input


def get_opds(dt=True, ind=True, sor=True):
    filename = 'opsd_germany_daily.csv'
    if os.path.isfile(filename):
        return pd.read_csv(filename)  # Returns local file if there is one
    else:
        power = pd.read_csv('https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv')
        power.columns = power.columns.str.lower()  # Lower case column names
        power.columns = power.columns.str.replace('+', '_')  # Replace '+' with '_'
        power = prep.create_index(power, 'date', datetime=dt, index=ind, sort=sor)  # Re-index df with datetime object
        power = power.fillna(0)  # Fill nulls with 0
        power.wind_solar = power.wind + power.solar  # Recreate wind_solar column
        power.to_csv('opsd_germany_daily.csv')  # Save to .csv file
        return power  # Return df
