import pandas as pd
import requests
import os
import json
from datetime import datetime, timedelta

# Configuration
GEOJSON_FILE = "live_news.geojson"

def get_gdelt_urls():
    """Fetches the URLs for the latest and the previous 15-minute GDELT update windows."""
    try:
        req = requests.get("http://data.gdeltproject.org/gdeltv2/lastupdate.txt")
        req.raise_for_status()
        lines = req.text.strip().split('\n')
        if not lines:
            return None, None
            
        # First line is the export CSV. Format: SIZE HASH URL
        export_line = lines[0]
        parts = export_line.split(' ')
        if len(parts) >= 3:
            latest_url = parts[2]
            
            # Extract timestamp from URL: e.g., http://data.gdeltproject.org/gdeltv2/20260226221500.export.CSV.zip
            filename = latest_url.split('/')[-1]
            ts_str = filename.split('.')[0]
            
            try:
                dt = datetime.strptime(ts_str, "%Y%m%d%H%M%S")
                # Subtract 15 minutes to get the previous window
                prev_dt = dt - timedelta(minutes=15)
                prev_ts_str = prev_dt.strftime("%Y%m%d%H%M%S")
                prev_url = f"http://data.gdeltproject.org/gdeltv2/{prev_ts_str}.export.CSV.zip"
                return latest_url, prev_url
            except ValueError:
                pass
    except Exception as e:
        print(f"Error getting update URLs: {e}")
    return None, None

def process_gdelt_url(url, columns):
    """Downloads and processes a single GDELT CSV export URL returning a list of GeoJSON features."""
    filename = url.split('/')[-1]
    features = []
    
    try:
        print(f"Fetching: {url}")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        with open(filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                
        # Read the downloaded zip file directly using pandas
        df = pd.read_csv(filename, sep='\t', names=columns, compression='zip', low_memory=False)
        
        # Drop rows without coordinates
        df = df.dropna(subset=['ActionGeo_Lat', 'ActionGeo_Long'])
        
        for _, row in df.iterrows():
            # Check for valid coordinate types to avoid serialization issues
            try:
                lat = float(row['ActionGeo_Lat'])
                lon = float(row['ActionGeo_Long'])
            except (ValueError, TypeError):
                continue
                
            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [lon, lat]
                },
                "properties": {
                    "SourceURL": row['SourceURL'] if pd.notnull(row['SourceURL']) else "",
                    "EventCode": row['EventCode'],
                    "Tone": row['AvgTone']
                }
            }
            features.append(feature)
            
        print(f"Processed {len(features)} events from {filename}")
        
    except Exception as e:
        print(f"Error processing {url}: {e}")
        
    finally:
        # Clean up the downloaded file
        if os.path.exists(filename):
            os.remove(filename)
            
    return features

def fetch_data():
    print("Finding the last two 15-minute update windows from GDELT...")
    latest_url, prev_url = get_gdelt_urls()
    
    if not latest_url or not prev_url:
        print("Failed to get GDELT URLs. Exiting.")
        return

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

    all_features = []
    
    # Process the latest 15-minute window
    all_features.extend(process_gdelt_url(latest_url, columns))
    
    # Process the previous 15-minute window
    all_features.extend(process_gdelt_url(prev_url, columns))
    
    total_events = len(all_features)
    print(f"Total events found across both windows: {total_events}")
    
    # Safety Switch
    if total_events == 0:
        print("Keeping old data.")
        return
        
    # Convert combined events to a single FeatureCollection
    geojson = {"type": "FeatureCollection", "features": all_features}
    
    # Overwrite the live_news.geojson with the new merged events
    with open(GEOJSON_FILE, 'w') as f:
        json.dump(geojson, f)
        
    print(f"Success! Saved {total_events} events to {GEOJSON_FILE}.")

if __name__ == "__main__":
    fetch_data()
