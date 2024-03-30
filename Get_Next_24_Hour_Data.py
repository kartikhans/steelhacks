from prophet import Prophet
from prophet.serialize import model_from_json, model_to_json
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException
import pandas as pd

try:
    with open('serialized_model.json', 'r') as fin:
        model = model_from_json(fin.read())
except Exception as e:
    raise Exception("Error loading model: " + str(e))

def load_and_combine_data(zone, years):
    dfs = [pd.read_csv(f"{zone}_{year}_daily.csv") for year in years]
    for df in dfs:
        df['zone'] = zone
    return pd.concat(dfs)

combined_df = pd.concat([
    load_and_combine_data('US', ['2021', '2022', '2023']),
    load_and_combine_data('BR', ['2021', '2022', '2023'])
])

combined_df.ffill(inplace=True)

def get_forecast(df):
    model = Prophet()
    zone = df['zone'].iloc[0]
    df.rename(columns = {'Datetime (UTC)': 'ds','Carbon Intensity gCO₂eq/kWh (direct)':'y'},inplace = True)
    model.fit(df)
    future = model.make_future_dataframe(periods=24, freq = 'H')
    forecast = model.predict(future)
    forecast['zone'] = zone
    with open('steelhacks/data.csv' , 'a') as f:
        forecast[['ds','yhat','yhat_lower','yhat_upper','zone']].tail(24).to_csv(f, header=True, index=False)
    

def get_data_for_zone(zone_names):
    try:
        combined_df = pd.concat([load_and_combine_data(zone, ['2021', '2022', '2023']) for zone in zone_names])
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Zone data not found.")
    combined_df.ffill(inplace=True)
    combined_df.groupby('zone')[['Datetime (UTC)','Carbon Intensity gCO₂eq/kWh (direct)','zone']].apply(get_forecast)

get_data_for_zone(['US', 'BR'])


# @app.get("/predict_hourly/")
# def predict_hourly(date: str):
#     # Validate the date format
#     try:
#         user_date = datetime.strptime(date, '%Y-%m-%d')
#     except ValueError:
#         raise HTTPException(status_code=400, detail="Invalid date format. Expected YYYY-MM-DD.")

#     # Check if user_date is in the past
#     if user_date < datetime.now():
#         raise HTTPException(status_code=400, detail="Invalid date. Date is in the past.")

#     # Create a dataframe with hourly intervals from now to user_date
#     future = pd.DataFrame({'ds': pd.date_range(start=datetime.now(), end=user_date, freq='H')})

#     # Predict
#     try:
#         forecast = model.predict(future)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail="Error making prediction: " + str(e))

#     return forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].to_dict()


# @app.get("/predict/{date}")
# def predict(date: str):
#     try:
#         user_date = datetime.strptime(date, '%Y-%m-%d')
#         future = pd.DataFrame({'ds': [user_date]})

#         forecast = model.predict(future)

#         return forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].to_dict()
#     except Exception as e:
#         raise HTTPException(status_code=400, detail=str(e))

# @app.exception_handler(Exception)
# def handle_exception(request, exc):
#     return {"error": str(exc)}