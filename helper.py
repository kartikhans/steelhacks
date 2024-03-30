from  datetime import datetime
import pandas as pd
def create_df(carbon_history, zone_name):
    zone_c, zone_n, carbon_intensity, hour_of_the_day = [], [], [], []
    for data in carbon_history:
        date_object = datetime.fromisoformat(data.get('datetime')[:-1])
        hour_of_the_day.append(date_object.hour)
        zone_c.append(data.get('zone'))
        zone_n.append(zone_name)
        carbon_intensity.append(data.get('carbonIntensity') or 0)
    return pd.DataFrame({
        'zone_code': zone_c,
        'zone_name': zone_n,
        'carbon_intensity': carbon_intensity,
        'hour_of_the_day': hour_of_the_day
    })