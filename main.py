import requests
import pandas as pd
import gspread
import gspread_pandas
from gspread_pandas import Spread, Client

#get metabse session id
headers = {'Content-Type': 'application/json'}
payload = {'username': '{your_metabase_id}', 'password': '{your_metabase_pw}'}
url = '{your_metabase_uri}'
r = requests.post(url, json=payload, headers=headers)
x_id = r.json()['id']

query_pair = {"{your_metabase_question_1}":'{your_gsheet_sheetname_1}',"{your_metabase_question_2}":'{your_gsheet_sheetname_2}'}
        
class MetabaseQuerytoGS:


    def metabase_get(q):
        headers = {'Content-Type': 'application/json', 'X-Metabase-Session':x_id}
        url = '{your_metabase_uri}' + q + '/query'
        response = requests.post(url, headers=headers)
        rows = response.json()['data']['rows']
        data = pd.DataFrame(rows)
        columns = response.json()['data']['cols']
        col=[]
        for i in columns:
            col.append(i['display_name'])
        data.columns = col
        return data

    def df2gs(data, sheet):
        try:
            c = gspread_pandas.conf.get_config(conf_dir='./', file_name='google_secret.json')
            print('config get')
            spread = Spread('{your_gsheetname}', config=c)
            print('spreaded')
            spread.df_to_sheet(data, index=False, sheet=sheet, start='A1', replace=True)
            print("done")
        except:
            "spread not done"

def main(self):
    for x, y in query_pair.items():
        data = MetabaseQuerytoGS.metabase_get(x) 
        MetabaseQuerytoGS.df2gs(data, y)
    return {}
        

if __name__ == '__main__':
    main(None)
