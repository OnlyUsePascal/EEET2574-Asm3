import pandas as pd
import numpy as np
import random

# Function to generate a synthetic dataset with challenging data
def generate_challenging_dataset(num_rows):
    # Define possible values for categorical and textual fields
    cities = ["da nang", "ha noi", "ho chi minh city"]
    weather_descriptions = ["clear sky", "few clouds", "scattered clouds", "rain", "thunderstorm"]
    pollution_levels = ["Low", "Moderate", "High"]
    event_descriptions = ["Stationary traffic", "Heavy congestion", "Queuing traffic", "Road maintenance", "Closed", "Accident", "Other"]
    weather_main = ["Clear", "Clouds", "Rain", "Thunderstorm"]

    # Generate synthetic data
    data = {
        "city": [random.choice(cities) for _ in range(num_rows)],
        "cloudiness": np.random.randint(0, 101, num_rows),
        "co": np.random.uniform(800.0, 1500.0, num_rows) + np.random.normal(0, 50, num_rows),  # High noise
        "date": pd.date_range(start="2025-01-01", periods=num_rows).strftime('%Y-%m-%d').tolist(),
        "delay": np.clip(np.random.normal(150, 80, num_rows).astype(int), 0, 300),
        "event_code": np.random.randint(100, 110, num_rows),
        "event_desc": [random.choice(event_descriptions) for _ in range(num_rows)],
        "feels_like": np.random.uniform(290.0, 310.0, num_rows) + np.random.normal(0, 2, num_rows),
        "gb-defra-index": np.clip(np.random.normal(5, 4, num_rows).astype(int), 1, 10),
        "hour": np.random.randint(0, 24, num_rows),
        "humidity": np.clip(np.random.normal(60, 20, num_rows).astype(int), 20, 100),
        "iconCategory": np.random.randint(1, 10, num_rows),
        "latitude": np.random.uniform(15.0, 18.0, num_rows) + np.random.normal(0, 0.1, num_rows),
        "length": np.random.uniform(50.0, 300.0, num_rows) + np.random.normal(0, 10, num_rows),
        "longitude": np.random.uniform(107.0, 110.0, num_rows) + np.random.normal(0, 0.1, num_rows),
        "magnitudeOfDelay": np.random.randint(1, 5, num_rows),
        "minute": np.random.randint(0, 60, num_rows),
        "o3": np.random.uniform(50.0, 150.0, num_rows) + np.random.normal(0, 10, num_rows),
        "pm10": np.random.uniform(50.0, 150.0, num_rows) + np.random.normal(0, 10, num_rows),
        "pm2_5": np.random.uniform(50.0, 150.0, num_rows) + np.random.normal(0, 10, num_rows),
        "pollution_level": [random.choice(pollution_levels) for _ in range(num_rows)],
        "pressure": np.random.uniform(1000.0, 1025.0, num_rows) + np.random.normal(0, 5, num_rows),
        "so2": np.random.uniform(10.0, 30.0, num_rows) + np.random.normal(0, 3, num_rows),
        "temperature": np.random.uniform(290.0, 310.0, num_rows) + np.random.normal(0, 2, num_rows),
        "us-epa-index": np.clip(np.random.normal(3, 1.5, num_rows).astype(int), 1, 5),
        "uv": np.clip(np.random.normal(5, 3, num_rows).astype(int), 0, 10),
        "visibility": np.clip(np.random.normal(15000, 7000, num_rows).astype(int), 5000, 20000),
        "weather_desc": [random.choice(weather_descriptions) for _ in range(num_rows)],
        "wind_deg": np.random.randint(0, 360, num_rows),
        "weather_main": [random.choice(weather_main) for _ in range(num_rows)],
        "wind_speed": np.random.uniform(0.5, 10.0, num_rows) + np.random.normal(0, 1, num_rows)
    }

    # Create a DataFrame
    df = pd.DataFrame(data)

    return df

# Generate and save challenging dataset
challenging_dataset = generate_challenging_dataset(num_rows= 8386)
challenging_dataset.to_csv("challenging_dataset.csv", index=False)
print("Challenging synthetic dataset generated and saved as 'challenging_dataset.csv'")
