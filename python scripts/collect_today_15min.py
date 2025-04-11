import requests
import pandas as pd
from datetime import datetime, timedelta
import os

# --- CONFIG ---
API_TOKEN = "478a759c0ef1ce824a835ddd699195ff0f66a9b5ae3b477e88a579c6b7ec47c5"
BASE_URL = "https://api.esios.ree.es/indicators/541"
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "x-api-key": API_TOKEN,
}

# --- TIME RANGE: Last 15 minutes ---
now = datetime.utcnow().replace(second=0, microsecond=0)
start = now - timedelta(minutes=15)

params = {
    "start_date": start.isoformat() + "Z",
    "end_date": now.isoformat() + "Z",
    "time_trunc": "quarter-hour"
}

# --- FETCH FROM API ---
print(f"📡 Fetching solar PV data from {start} to {now}")
res = requests.get(BASE_URL, headers=HEADERS, params=params)
res.raise_for_status()
data = res.json()["indicator"]["values"]

# --- CREATE DATAFRAME ---
df_new = pd.DataFrame(data)
df_new["datetime"] = pd.to_datetime(df_new["datetime"])
df_new = df_new.sort_values("datetime")

# --- DAILY FILE ---
date_str = now.strftime("%Y-%m-%d")
file_path = f"data/{date_str}.csv"
os.makedirs("data", exist_ok=True)

if os.path.exists(file_path):
    df_existing = pd.read_csv(file_path, parse_dates=["datetime"])
    df_combined = pd.concat([df_existing, df_new]).drop_duplicates(subset=["datetime"]).sort_values("datetime")
else:
    df_combined = df_new

df_combined.to_csv(file_path, index=False)
print(f"✅ Updated {file_path} with {len(df_new)} new rows.")
