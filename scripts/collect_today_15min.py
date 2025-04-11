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

# --- Time setup (UTC-aware) ---
now = datetime.utcnow().replace(second=0, microsecond=0, tzinfo=timezone.utc)
today_str = now.strftime("%Y-%m-%d")
daily_file = f"data/{today_str}.csv"
os.makedirs("data", exist_ok=True)

# --- Determine fetch range ---
if os.path.exists(daily_file):
    df_existing = pd.read_csv(daily_file, parse_dates=["datetime"])
    if not df_existing.empty:
        last_dt = df_existing["datetime"].max()
        # Ensure last_dt is also timezone-aware
        if last_dt.tzinfo is None:
            last_dt = last_dt.replace(tzinfo=timezone.utc)
        start = last_dt + timedelta(minutes=15)
    else:
        start = now.replace(hour=0, minute=0)
else:
    df_existing = pd.DataFrame()
    start = now.replace(hour=0, minute=0)

end = now

# If up to date, skip
if start >= end:
    print("✅ No new data to fetch.")
    exit()

# --- Fetch from API ---
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

# --- Combine with existing (if any) ---
df_combined = pd.concat([df_existing, df_new]).drop_duplicates(subset=["datetime"]).sort_values("datetime")

# --- Save to daily CSV ---
df_combined.to_csv(daily_file, index=False)
print(f"✅ {len(df_new)} new rows added to {daily_file}")


