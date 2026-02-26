import requests
import sys
import io
import zipfile
import pandas as pd
import json
import time
from datetime import datetime

def get_latest_gdelt_export_url():
    url = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        for line in response.text.strip().split('\n'):
            if line.strip().endswith('.export.CSV.zip'):
                parts = line.split()
                if len(parts) >= 3:
                    return parts[2]
                
        print("Could not find the export.CSV.zip URL in the response.")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

def create_geojson_features(df):
    features = []
    for _, row in df.iterrows():
        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [row['Longitude'], row['Latitude']]
            },
            "properties": {
                "EventCode": row['EventCode'],
                "SourceURL": row['SourceURL']
            }
        }
        features.append(feature)
    return features

def process_gdelt_csv(url):
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            csv_filename = z.namelist()[0]
            with z.open(csv_filename) as csv_file:
                # GDELT v2 files are tab-separated despite the .CSV extension
                df = pd.read_csv(
                    csv_file,
                    sep='\t',
                    header=None,
                    usecols=[26, 48, 49, 60],
                    names=['EventCode', 'Latitude', 'Longitude', 'SourceURL']
                )
                # Clean: Drop rows where Latitude, Longitude, or SourceURL are missing/NaN/null
                df = df.dropna(subset=['Latitude', 'Longitude', 'SourceURL'])
                # Additionally drop if they are empty strings
                df = df[df['SourceURL'] != '']
                
                # Type Casting: Ensure Latitude and Longitude are explicitly cast as float
                # coerce errors so invalid numeric strings are converted to NaN then dropped
                df['Latitude'] = pd.to_numeric(df['Latitude'], errors='coerce')
                df['Longitude'] = pd.to_numeric(df['Longitude'], errors='coerce')
                
                # drop the resulting NaNs from coercion
                df = df.dropna(subset=['Latitude', 'Longitude'])
                
                print(f"Total valid rows saved: {len(df)}")
                # Create GeoJSON features
                features = create_geojson_features(df)
                
                # Bundle into FeatureCollection
                feature_collection = {
                    "type": "FeatureCollection",
                    "features": features
                }
                
                # Export to 'live_news.geojson'
                with open('live_news.geojson', 'w') as f:
                    json.dump(feature_collection, f, indent=2)
                    
                print(f"Successfully saved {len(features)} features to live_news.geojson")
                
    except Exception as e:
        print(f"Error processing data: {e}")

if __name__ == "__main__":
    print("Starting continuous GDELT fetcher...")
    while True:
        try:
            latest_url = get_latest_gdelt_export_url()
            if latest_url:
                process_gdelt_csv(latest_url)
            else:
                print("Failed to get latest URL this cycle.")
        except Exception as e:
            print(f"Error in main loop: {e}")
            
        print(f"Update successful at: {datetime.now()}")
        print("Waiting 15 minutes for next fetch...")
        time.sleep(900)
