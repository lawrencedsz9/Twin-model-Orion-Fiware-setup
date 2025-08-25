import pandas as pd
import requests
import time

# --- Config ---
ORION_URL = "http://localhost:1026/v2/entities/Room1/attrs"
HEADERS = {"Content-Type": "application/json"}
SLEEP_SECS = 1  # how many seconds between each row update

# --- Load dataset ---
# Your file has an extra index column before "date"
df = pd.read_csv("C:/Users/lawre/Documents/Twin model/datatest.txt",
                 index_col=0, parse_dates=["date"])

expected = {"date", "Temperature", "Humidity", "Light", "CO2", "HumidityRatio", "Occupancy"}
missing = expected - set(df.columns)
if missing:
    raise ValueError(f"Missing columns in dataset: {missing}")

# --- Replay loop ---
with requests.Session() as s:
    s.headers.update(HEADERS)

    for _, row in df.iterrows():
        # Build payload (NGSI-v2 attrs with explicit types)
        payload = {
            "temperature":    {"type": "Number",   "value": float(row["Temperature"])},
            "humidity":       {"type": "Number",   "value": float(row["Humidity"])},
            "light":          {"type": "Number",   "value": float(row["Light"])},
            "co2":            {"type": "Number",   "value": float(row["CO2"])},
            "humidityRatio":  {"type": "Number",   "value": float(row["HumidityRatio"])},
            "occupancy":      {"type": "Integer",  "value": int(row["Occupancy"])},
            "dateObserved":   {"type": "DateTime", "value": row["date"].isoformat()}
        }

        r = s.patch(ORION_URL, json=payload)
        if r.status_code not in (204, 200):
            print(f"Failed at {row['date']} -> {r.status_code}, {r.text}")
        else:
            print(f"Sent row at {row['date']}")

        time.sleep(SLEEP_SECS)

print("Dataset replay finished")
