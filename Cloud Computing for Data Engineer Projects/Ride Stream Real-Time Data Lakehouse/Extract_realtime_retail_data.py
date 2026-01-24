import requests
import json

url = "https://data.cityofnewyork.us/resource/gkne-dk5s.json"
params = {"$limit": 100}

response = requests.get(url, params=params)
data = response.json()

with open("nyc_taxi_sample.json", "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2)

print(f"✅ Saved {len(data)} taxi records into nyc_taxi_sample.json")
