# from bs4 import BeautifulSoup
from prophet import Prophet
from prophet.serialize import model_from_json, model_to_json
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException
# import requests
import pandas as pd
# import pickle

app = FastAPI()


# df1 = pd.read_csv("US_2021_daily.csv")
# df2 = pd.read_csv("US_2022_daily.csv")
# df3 = pd.read_csv("US_2023_daily.csv")
# combined_df = pd.concat([df1,df2,df3])
# combined_df = combined_df[['Datetime (UTC)','Carbon Intensity gCO₂eq/kWh (direct)']]
# combined_df = combined_df.ffill()
# combined_df = combined_df.rename(columns={'Datetime (UTC)':'ds','Carbon Intensity gCO₂eq/kWh (direct)':'y'})


# model = Prophet()
# model.fit(combined_df)

# with open('serialized_model.json', 'w') as fout:
#     fout.write(model_to_json(model))  # Save model

@app.get("/predict_hourly/{date}")
def predict_hourly(date: str):
    try:
        user_date = datetime.strptime(date, '%Y-%m-%d')
        

        if user_date < datetime.now():
            raise Exception("Invalid date")

        else:
            with open('serialized_model.json', 'r') as fin:
                model = model_from_json(fin.read()) 
            future = pd.DataFrame({'ds': pd.date_range(start=datetime.now(), end=user_date, freq='H')})

            forecast = model.predict(future)

            return forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].to_dict()
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/predict/{date}")
def predict(date: str):
    try:
        user_date = datetime.strptime(date, '%Y-%m-%d')
        future = pd.DataFrame({'ds': [user_date]})

        with open('serialized_model.json', 'r') as fin:
            model = model_from_json(fin.read()) 
        forecast = model.predict(future)

        return forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].to_dict()
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.exception_handler(Exception)
def handle_exception(request, exc):
    return {"error": str(exc)}