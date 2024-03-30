from fastapi import FastAPI
from dotenv import load_dotenv
import os
import requests
from datetime import datetime
import pandas as pd
import pytz

from helper import create_df
app = FastAPI()
load_dotenv()


headers = {'Authorization' : f'Token {os.getenv("API_TOKEN")}'}

def get_data(zone, zone_name):
    params = {
        'zone': zone
    }
    req = requests.get(os.getenv("BASE_URL") + '/v3/carbon-intensity/history', params=params, headers=headers)
    data = req.json() if req.status_code == 200 else {}

    return create_df(data.get('history', []), zone_name)


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/zones")
def get_zones():
    response = requests.get(os.getenv("BASE_URL") + '/v3/zones', headers=headers)
    zone_data = []
    if response.status_code == 200:
        for key, value in response.json().items():
            zone_data.append((key, value.get('zoneName')))
    return {'total_zones': len(zone_data),
            'fetch_time': datetime.now().isoformat(),
            'data': zone_data}

@app.get("/generate_predictions")
def gen_predictions():
    zones = get_zones()['data']
    df = pd.DataFrame(columns=['zone_code', 'zone_name', 'carbon_intensity', 'hour_of_the_day'])

    request_time = datetime.now().isoformat()
    for zone in zones:
        individual_df = get_data(zone[0], zone[1])
        df = df.append(individual_df, ignore_index=True)
    df = df.sort_values(by=['hour_of_the_day', 'carbon_intensity'], ascending=False)
    df.to_csv(f'data/predictions.csv')
    return 'success'

@app.get('/get_prediction')
def get_prediction(time_from, time_to, timezone):
    time_from = int(time_from)
    time_to = int(time_to)
    df = pd.read_csv('data/predictions.csv')
    df = df[df['carbon_intensity'] != 0]
    offset = datetime.now(pytz.timezone(timezone)).utcoffset().total_seconds() / 60 / 60
    new_time_from = time_from - offset
    if new_time_from < 0:
        new_time_from += 24
    new_time_to = time_to - offset
    if new_time_to < 0:
        new_time_to += 24

    final_data = {}

    while new_time_from != new_time_to:
        new_df = df[df['hour_of_the_day'] == int(new_time_from)].sort_values(by=['carbon_intensity'])
        data = []
        for index, row in new_df.head(10).iterrows():
            data.append({
                'zone_name': row.get('zone_name'),
                'zone_code': row.get('zone_code'),
                'carbon_intensity': row.get('carbon_intensity')
            })
        final_data[(new_time_from+offset)%24] = data
        new_time_from = (new_time_from + 1)%24
    # for i in range(int(time_from) + offset, int(time_to)):
    #     new_df = df[df['hour_of_the_day'] == i].sort_values(by=['carbon_intensity'])
    #     data = []
    #     for index, row in new_df.head(10).iterrows():
    #         data.append({
    #         'zone_name': row.get('zone_name'),
    #         'zone_code': row.get('zone_code'),
    #         'carbon_intensity': row.get('carbon_intensity')
    #     })
    #     final_data[i] = data
    return {
        'data': final_data
    }

@app.get('/get_prediction_by_hour')
def get_prediction_by_hour(hours, timezone):
    df = pd.read_csv('data/predictions.csv')
    df = df[df['carbon_intensity'] != 0]
    df = df.sort_values(by=['carbon_intensity'], ascending=True).drop_duplicates(subset=['hour_of_the_day'], keep='first')
    final_data = {}
    offset = datetime.now(pytz.timezone(timezone)).utcoffset().total_seconds() / 60 / 60

    for index, row in df.head(int(hours)).iterrows():
        final_data[int(row['hour_of_the_day']) + int(offset)] = {
            'zone_name': row.get('zone_name'),
            'zone_code': row.get('zone_code'),
            'carbon_intensity': row.get('carbon_intensity')
        }
    return {
        'data': final_data
    }
