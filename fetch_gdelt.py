import pandas as pd
import requests
import gzip
import os
import json

# Configuration
GDELT_URL = "http://data.gdeltproject.org/gdeltv2/last15min.export.CSV.gz"
CSV_FILE = "latest.csv.gz"
GEOJSON_FILE = "live_news.geojson"

def fetch_data():
    print("Fetching latest 15-minute update from GDELT...")
    response = requests.get(GDELT_URL)
    with open(CSV_FILE, 'wb') as f:
        f.write(response.content)

    # Column names for GDELT v2
    columns = [
        "GlobalEventID", "Day", "MonthYear", "Year", "FractionDate",
        "Actor1Code", "Actor1Name", "Actor1CountryCode", "Actor1KnownGroupCode", "Actor1EthnicCode",
        "Actor1Religion1Code", "Actor1Religion2Code", "Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code",
        "Actor2Code", "Actor2Name", "Actor2CountryCode", "Actor2KnownGroupCode", "Actor2EthnicCode",
        "Actor2Religion1Code", "Actor2Religion2Code", "Actor2Type1Code", "Actor2Type2Code", "Actor2Type3Code",
        "IsRootEvent", "EventCode", "EventBaseCode", "EventRootCode", "QuadClass",
        "GoldsteinScale", "NumMentions", "NumSources", "NumArticles", "AvgTone",
        "Actor1Geo_Type", "Actor1Geo_FullName", "Actor1Geo_CountryCode", "Actor1Geo_ADM1Code", "Actor1Geo_Lat", "Actor1Geo_Long", "Actor1Geo_FeatureID",
        "ActionGeo_Type", "ActionGeo_FullName", "ActionGeo_CountryCode", "ActionGeo_ADM1Code", "ActionGeo_Lat", "ActionGeo_Long", "ActionGeo_FeatureID",
        "DATEADDED", "SourceURL"
    ]

    df = pd.read_csv(CSV_FILE, sep='\t', names=columns, compression='gzip', low_memory=False)

    # Drop rows without coordinates
    df = df.dropna(subset=['ActionGeo_Lat', 'ActionGeo_Long'])

    # Convert to GeoJSON
    features = []
    for _, row in df.iterrows():
        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [float(row['ActionGeo_Long']), float(row['ActionGeo_Lat'])]
            },
            "properties": {
                "SourceURL": row['SourceURL'],
                "EventCode": row['EventCode'],
                "Tone": row['AvgTone']
            }
        }
        features.append(feature)

    geojson = {"type": "FeatureCollection", "features": features}

    with open(GEOJSON_FILE, 'w') as f:
        json.dump(geojson, f)
    
    print(f"Success! Processed {len(features)} events.")

if __name__ == "__main__":
    fetch_data()
