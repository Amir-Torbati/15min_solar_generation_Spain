import requests
import pandas as pd
from datetime import datetime, timedelta
import os

API_TOKEN = "478a759c0ef1ce824a835ddd699195ff0f66a9b5ae3b477e88a579c6b7ec47c5"
BASE_URL = "https://api.esios.ree.es/indicators/541"
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "x-api-key": API_TOKEN,
}

# ✅ Change: now fetch last 15 minutes
now = datetime.utcnow().replace(second=0, microsecond=0)
fifteen_minutes_ago = now - timedelta(minutes=15)

start_date = fifteen_minutes_ago.isoformat() + "Z"
end_date = now.isoformat() + "Z"

params = {
    "start_date": start_date,
    "end_date": end_date,
    "time_trunc": "quarter-hour"  # ✅ quarter-hour = 15-min intervals
}

res = requests.get(BASE_URL, headers=HEADERS, params=params)
res.raise_for_status()
data = res.json()["indicator"]["values"]

df_new = pd.DataFrame(data)
df_new["datetime"] = pd.to_datetime(df_new["datetime"])

file_path = "data/solar_database.csv"
os.makedirs("data", exist_ok=True)

if os.path.exists(file_path):
    df_existing = pd.read_csv(file_path, parse_dates=["datetime"])
    df_combined = pd.concat([df_existing, df_new]).drop_duplicates(subset=["datetime"]).sort_values("datetime")
else:
    df_combined = df_new

df_combined.to_csv(file_path, index=False)
print(f"✅ Saved data from {start_date} to {end_date}")

