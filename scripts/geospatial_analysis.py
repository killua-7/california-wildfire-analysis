import psycopg2
import os
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import folium
from dotenv import load_dotenv
import warnings
from datetime import datetime, timedelta

# Ignore warnings
warnings.filterwarnings('ignore') 

try:
    load_dotenv()

    # Connect to PostgreSQL
    conn = psycopg2.connect(
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT')
    )

    print("[SUCCESS] Successfully connected to PostgreSQL")

    # Load data into a pandas DataFrame
    query = "SELECT latitude, longitude, brightness, frp, acq_date, confidence FROM wildfires;"
    data = pd.read_sql(query, conn)

    # Close the connection
    conn.close()

    # Convert 'acq_date' to datetime
    data['acq_date'] = pd.to_datetime(data['acq_date'])

    # Filter data for high-confidence fires (confidence > 80)
    data = data[data['confidence'] > 80]

    # Filter data for the last year
    one_year_ago = pd.to_datetime(datetime.now() - timedelta(days=1460))
    data = data[data['acq_date'] >= one_year_ago]

    # Convert latitude and longitude to a geospatial DataFrame
    geometry = [Point(xy) for xy in zip(data['longitude'], data['latitude'])]
    gdf = gpd.GeoDataFrame(data, geometry=geometry)

    # Create a map centered on California
    california_map = folium.Map(location=[36.7783, -119.4179], zoom_start=6)

    # Add wildfire locations to the map
    for _, row in gdf.iterrows():
        folium.CircleMarker(
            location=[row['latitude'], row['longitude']],
            radius=3,
            color='red',
            fill=True,
            fill_color='red',
            fill_opacity=0.6,
            popup=f"Brightness: {row['brightness']}, FRP: {row['frp']}, Date: {row['acq_date'].strftime('%Y-%m-%d')}"
        ).add_to(california_map)

    # Save the map to an HTML file
    california_map.save('wildfire_map.html')

    print("[SUCCESS] Geospatial analysis complete! Check 'wildfire_map.html' for the visualization.")

except Exception as e:
    print(f"ERROR: {e}")