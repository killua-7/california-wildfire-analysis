import psycopg2
import os
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

try:
    # Load the cleaned dataset
    data = pd.read_csv('data/processed/cleaned_modis_data.csv')

    # Convert 'acq_date' to datetime
    data['acq_date'] = pd.to_datetime(data['acq_date']).dt.date

    load_dotenv()

    # Connect to PostgreSQL
    conn = psycopg2.connect(
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT')
    )
    cursor = conn.cursor()
    print("[SUCCESS] Successfully connected to PostgreSQL")

    # Insert data into the table
    for _, row in data.iterrows():
        cursor.execute(
            """
            INSERT INTO wildfires (latitude, longitude, brightness, frp, acq_date, confidence)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (row['latitude'], row['longitude'], row['brightness'], row['frp'], row['acq_date'], row['confidence'])
        )

    # Commit the transaction and close the connection
    conn.commit()
    cursor.close()
    conn.close()

    print("[SUCCESS] Data successfully loaded into PostgreSQL!")

except Exception as e:
    print(f"Error: {e}")