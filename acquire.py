import requests
import pandas as pd


def scrape_data(url='link'):
    """ This function is used to pull data from https://swapi.dev/.
        url - Url for the FIRST page of the data."""
    response = requests.get(url)
    data = response.json()
    df = pd.DataFrame(data['results'])
    page = data.copy()
    while page['next'] != None:
        page = requests.get(page['next']).json()
        df = pd.concat([df, pd.DataFrame(page['results'])], ignore_index=True)
    return df



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

