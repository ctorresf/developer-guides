import requests
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
API_KEY = os.getenv("OPENWEATHER_API_KEY")
INPUT_FILE = "data/cities.csv"
OUTPUT_BASE_PATH = "./datalake/weather_forecast"

def validate_config():
    """Ensure all required configurations are present."""
    if not API_KEY:
        raise ValueError("CRITICAL: OPENWEATHER_API_KEY not found in environment.")
    if not os.path.exists(INPUT_FILE):
        raise FileNotFoundError(f"CRITICAL: {INPUT_FILE} not found.")

def process_forecast_data(json_data, city_name, country_code):
    """
    Extracts relevant fields from the nested JSON response.
    Normalizes the 3-hour/5-day forecast into a flat list.
    """
    records = []
    # OpenWeather returns a list of 40 forecast points
    forecast_list = json_data.get('list', [])
    
    for item in forecast_list:
        row = {
            "city": city_name,
            "country": country_code,
            "forecast_time": item.get('dt_txt'),
            "temp": item['main'].get('temp'),
            "temp_min": item['main'].get('temp_min'),
            "temp_max": item['main'].get('temp_max'),
            "humidity": item['main'].get('humidity'),
            "weather_main": item['weather'][0].get('main'),
            "weather_description": item['weather'][0].get('description'),
            "wind_speed": item['wind'].get('speed'),
            "precipitation_probability": item.get('pop', 0),
            "ingestion_at": datetime.utcnow().isoformat()
        }
        records.append(row)
        
    return pd.DataFrame(records)

def run_ingestion_pipeline():
    validate_config()
    
    # Read operational cities
    cities_df = pd.read_csv(INPUT_FILE)
    execution_date = datetime.now().strftime("%Y-%m-%d")
    all_forecasts = []

    for _, row in cities_df.iterrows():
        city = row['city_name']
        country = row['country_code']
        
        print(f"🛰️ Fetching forecast for {city}, {country}...")
        
        # OpenWeather API Call (Metric units for Celsius)
        url = f"https://api.openweathermap.org/data/2.5/forecast?lat={row['lat']}&lon={row['lon']}&appid={API_KEY}&units=metric"
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            
            # Clean and flatten the data
            clean_df = process_forecast_data(response.json(), city, country)
            all_forecasts.append(clean_df)
            
        except Exception as e:
            print(f"❌ Failed to process {city}: {e}")

    # Consolidate and Save to Parquet
    if all_forecasts:
        final_df = pd.concat(all_forecasts, ignore_index=True)
        
        # Create partitioned directory path
        output_dir = os.path.join(OUTPUT_BASE_PATH, f"p_ingestion_date={execution_date}")
        os.makedirs(output_dir, exist_ok=True)
        
        output_file = os.path.join(output_dir, "forecast.parquet")
        
        # Save using Parquet format (efficient columnar storage)
        final_df.to_parquet(output_file, index=False, engine='pyarrow')
        
        print(f"\n✅ Pipeline completed successfully.")
        print(f"📁 Data saved to: {output_file}")
        print(f"📊 Total records processed: {len(final_df)}")

if __name__ == "__main__":
    run_ingestion_pipeline()