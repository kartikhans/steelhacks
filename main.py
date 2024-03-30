# from bs4 import BeautifulSoup
from prophet import Prophet
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException
# import requests
import pandas as pd
# import pickle

app = FastAPI()


df1 = pd.read_csv("US_2021_daily.csv")
df2 = pd.read_csv("US_2022_daily.csv")
df3 = pd.read_csv("US_2023_daily.csv")
combined_df = pd.concat([df1,df2,df3])
combined_df = combined_df[['Datetime (UTC)','Carbon Intensity gCO₂eq/kWh (direct)']]
combined_df = combined_df.ffill()
combined_df = combined_df.rename(columns={'Datetime (UTC)':'ds','Carbon Intensity gCO₂eq/kWh (direct)':'y'})


model = Prophet()
model.fit(combined_df)

@app.get("/predict/{date}")
def predict(date: str):
    try:
        # Convert the date to datetime
        user_date = datetime.strptime(date, '%Y-%m-%d')

        # Create a future dataframe with the user's date
        future = pd.DataFrame({'ds': [user_date]})

        # Predict
        forecast = model.predict(future)

        # Return the forecast for the user's date
        return forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].to_dict()
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.exception_handler(Exception)
def handle_exception(request, exc):
    return {"error": str(exc)}