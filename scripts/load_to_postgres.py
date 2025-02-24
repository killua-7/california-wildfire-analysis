import psycopg2
import pandas as pd
from datetime import datetime


try:
    # Load the cleaned dataset
    data = pd.read_csv('data/processed/cleaned_modis_data.csv')

    # Convert 'acq_date' to datetime
    data['acq_date'] = pd.to_datetime(data['acq_date']).dt.date

    # Connect to PostgreSQL
    conn = psycopg2.connect(
        dbname="wildfire_db",
        user="postgres",  # Replace with your PostgreSQL username
        password="postgres",  # Replace with your PostgreSQL password
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()

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

    print("Data successfully loaded into PostgreSQL!")

except Exception as e:
    print(f"Error: {e}")