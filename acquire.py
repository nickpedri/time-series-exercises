import requests
import pandas as pd


def scrape_data(url='link'):
    response = requests.get(url)
    data = response.json()
    df = pd.DataFrame(data['results'])
    page = data.copy()
    while page['next'] != None:
        page = requests.get(page['next']).json()
        df = pd.concat([df, pd.DataFrame(page['results'])], ignore_index=True)
    return df
