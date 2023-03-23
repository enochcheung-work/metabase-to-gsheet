import requests
import pandas as pd
import gspread
import gspread_pandas
from gspread_pandas import Spread, Client
import json

#get metabase session id
headers = {'Content-Type': 'application/json'}
with open('payload.json') as f:
    payload = json.load(f)
url = 'https://YOURURL/api/session'

def get_x_id():
    r = requests.post(url, json=payload, headers=headers)
    x_id = r.json()['id']
    return x_id

def get_data_from_metabase(query_id, x_id):
    headers = {'Content-Type': 'application/json', 'X-Metabase-Session': x_id}
    url = f'https://YOURURL/api/card/{query_id}/query'
    response = requests.post(url, headers=headers)
    rows = response.json()['data']['rows']
    data = pd.DataFrame(rows)
    columns = response.json()['data']['cols']
    col=[]
    for i in columns:
        col.append(i['display_name'])
    data.columns = col
    return data

def write_to_gs(data, sheet_name):
    try:
        c = gspread_pandas.conf.get_config(conf_dir='./', file_name='google_secret.json')
        spread = Spread('metabase_query', config=c)
        spread.df_to_sheet(data, index=False, sheet=sheet_name, start='A1', replace=True)
        print(f'Successfully wrote data to sheet "{sheet_name}"')
    except Exception as e:
        print(f'Error writing data to sheet "{sheet_name}": {e}')

def main(request):
    with open('query_pair.json') as f:
        query_pair = json.load(f)
    try:
        with open('/tmp/x_id.json') as f:
            x_id = json.load(f)['id']
        # check if the x_id is still valid
        headers = {'Content-Type': 'application/json', 'X-Metabase-Session': x_id}
        url = 'https://YOURURL/api/user/current'
        response = requests.get(url, headers=headers)
        if response.status_code == 403:
            # if the session is expired, get a new x_id
            x_id = get_x_id()
            with open('/tmp/x_id.json', 'w') as f:
                json.dump({'id': x_id}, f)
    except FileNotFoundError:
        x_id = get_x_id()
        with open('/tmp/x_id.json', 'w') as f:
            json.dump({'id': x_id}, f)
    for query_id, sheet_name in query_pair.items():
        data = get_data_from_metabase(query_id, x_id)
        write_to_gs(data, sheet_name)
    return f"Function completed successfully."
