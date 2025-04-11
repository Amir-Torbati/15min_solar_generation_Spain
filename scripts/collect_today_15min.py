import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
import os

# --- Config ---
API_TOKEN = "478a759c0ef1ce824a835ddd699195ff0f66a9b5ae3b477e88a579c6b7ec47c5"
BASE_URL = "https://api.esios.ree.es/indicators/541"
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "x-api-key": API_TOKEN,
}

# --- Time setup (always use UTC-aware datetimes) ---
now = datetime.utcnow().replace(second=0, microsecond=0, tzinfo=timezone.utc)
today_str = now.strftime("%Y-%m-%d")
daily_file = f"data/{today_str}.csv"
os.makedirs("data", exist_ok=True)

# --- Always fetch from 00:00 UTC today ---
start = now.replace(hour=0, minute=0)
end = now

# --- Load existing file (if it exists) ---
df_existing = pd.DataFrame()
if os.path.exists(daily_file):
    df_existing = pd.read_csv(daily_file, parse_dates=["datetime"])

# --- Fetch full range from 00:00 → now ---
params = {
    "start_date": start.isoformat(),
    "end_date": end.isoformat(),
    "time_trunc": "quarter-hour"
}

print(f"📡 Fetching solar PV data from {start} to {end}...")

res = requests.get(BASE_URL, headers=HEADERS, params=params)
res.raise_for_status()
data = res.json()["indicator"]["values"]

df_new = pd.DataFrame(data)
df_new["datetime"] = pd.to_datetime(df_new["datetime"])
df_new = df_new.sort_values("datetime")

# --- Combine (and deduplicate) ---
df_combined = pd.concat([df_existing, df_new])
df_combined = df_combined.drop_duplicates(subset=["datetime"]).sort_values("datetime")

# --- Save back to CSV ---
df_combined.to_csv(daily_file, index=False)

print(f"✅ Synced {len(df_combined)} rows in {daily_file}")
