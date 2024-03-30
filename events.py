from fastapi_utilities import repeat_every
from main import get_zones

@router.on_event('startup')
@repeat_every(seconds=3600)
def run_prediction_model():
    zones = get_zones()['data']
    for zone in zones:
        get_data_for_zone(zone[1])
        print(f'prediction model ran for {zone[1]}...')