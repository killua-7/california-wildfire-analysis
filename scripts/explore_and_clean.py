import pandas as pd

try:
    file_path = 'data/raw/fire_archive_M-C61_579068.csv'

    data = pd.read_csv(file_path)

    relevant_columns = ['latitude', 'longitude', 'brightness', 'frp', 'acq_date', 'confidence']
    data = data[relevant_columns]

    # Drop rows with missing values
    data1 = data.dropna()

    # Convert data types
    data['latitude'] = data['latitude'].astype(float)
    data['longitude'] = data['longitude'].astype(float)
    data['brightness'] = data['brightness'].astype(float)
    data['frp'] = data['frp'].astype(float)


    # Save the cleaned dataset
    data.to_csv('data/processed/cleaned_modis_data.csv', index=False)
except Exception as e:
    print(f"Error: {e}")