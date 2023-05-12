import csv
import requests
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
def fetch_api():
    params = {
        "latitude": "20.47",
        "longitude": "106.02",
        "hourly": "temperature_2m",
        "current_weather": "true",
        "timezone": "Asia/Bangkok"
    }
    url = "https://api.open-meteo.com/v1/forecast"
    response = requests.get(url, params)
    data = response.json()
    if response.status_code == 200:
        df = pd.DataFrame(data["hourly"])
        weather_forecast_file = (
            "du_bao_nhiet_do_tu_"
            + datetime.date.today().strftime("%d_%m_%Y")
            + "_den_"
            + (datetime.date.today() + relativedelta(days=6)).strftime("%d_%m_%Y")
            + ".csv"
        )
        collections = df.to_csv( r'./dags/data/weather_forcast' + weather_forecast_file, sep=',', header=True, index=False)
        print("Mark as successed")
    else:
        print("Error code: " + str(response.status_code))
        print("Re-check url and params: " + response.url)
