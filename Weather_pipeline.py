import requests
import pandas as pd
import snowflake.connector
from datetime import datetime
import json

from airflow.decorators import dag, task

# --- CONFIGURATION ---
# IMPORTANT: Replace these placeholders with your actual credentials!
# OpenWeatherMap
API_KEY = 'cb6f258472a6600efcf5574c4608cd5b'
CITIES = ['Chennai', 'Mumbai', 'Delhi', 'Bangalore', 'Tiruchirappalli']

# Snowflake
SNOWFLAKE_USER = 'sivaranchani'
SNOWFLAKE_PASSWORD = 'Sivaranchani@123'
SNOWFLAKE_ACCOUNT = 'ucozpth-ph46898' # e.g., abc12345.ap-south-1
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'
SNOWFLAKE_DATABASE = 'WEATHER_DB'
SNOWFLAKE_SCHEMA = 'PIPELINE'
SNOWFLAKE_TABLE = 'DAILY_WEATHER'

# --- DAG DEFINITION ---
@dag(
    dag_id='weather_etl_pipeline',
    start_date=datetime(2025, 1, 1), # Use a fixed start date in the past
    schedule='@daily',              # Correct argument name for schedule
    catchup=False,                  # Don't run for past missed schedules
    tags=['weather', 'etl'],
)
def weather_etl_dag():
    """
    An ETL pipeline that fetches weather data, transforms it, and loads it into Snowflake.
    """

    @task
    def extract():
        """Fetches weather data from the OpenWeatherMap API and returns it as a JSON string."""
        print("Starting data extraction...")
        all_weather_data = []
        for city in CITIES:
            url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}"
            try:
                response = requests.get(url, timeout=10) # Added timeout
                response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
                all_weather_data.append(response.json())
                print(f"Successfully fetched data for {city}.")
            except requests.exceptions.RequestException as e:
                print(f"Failed to fetch data for {city}. Error: {e}")
                # Optional: Decide if you want the DAG to fail if one city fails
                # raise e

        if not all_weather_data:
             raise ValueError("No weather data extracted, stopping pipeline.")

        # Airflow passes data between tasks as strings by default (using XComs)
        # Convert the list of dictionaries to a JSON string
        return json.dumps(all_weather_data)

    @task
    def transform(raw_data_json: str):
        """Cleans and transforms the raw weather data JSON string."""
        print("Starting data transformation...")
        try:
            raw_data = json.loads(raw_data_json) # Convert JSON string back to Python list
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON data: {e}")
            raise

        transformed_data = []
        for data in raw_data:
            # Basic check if the expected keys exist
            if 'main' in data and 'temp' in data['main'] and 'weather' in data and data['weather']:
                 temperature_celsius = round(data['main']['temp'] - 273.15, 2)
                 transformed_data.append({
                    'City': data.get('name', 'Unknown'), # Use .get for safety
                    'Temperature_C': temperature_celsius,
                    'Humidity_Percent': data['main'].get('humidity', None),
                    'Description': data['weather'][0].get('description', 'N/A'),
                    # *** CHANGE 1: Keep as datetime object ***
                    'Timestamp_UTC': datetime.utcnow()
                 })
            else:
                 print(f"Skipping record due to missing key data: {data.get('name', 'Unknown City')}")

        if not transformed_data:
            raise ValueError("No data transformed, stopping pipeline.")

        df = pd.DataFrame(transformed_data)
        print("\n--- Clean Weather Data ---")
        print(df)

        # *** CHANGE 2: Remove date_format argument ***
        # Convert DataFrame to JSON string format suitable for XComs
        # Pandas will serialize datetime objects in a standard way (usually ISO 8601)
        return df.to_json(orient='split')

    @task
    def load(clean_data_json: str):
        """Loads the transformed data JSON string into a Snowflake table."""
        print("Starting load to Snowflake...")
        try:
             # Pandas should automatically parse the standard datetime format back
            df = pd.read_json(clean_data_json, orient='split')
            # Explicitly convert to datetime if needed, though usually automatic
            df['Timestamp_UTC'] = pd.to_datetime(df['Timestamp_UTC'])
        except Exception as e: # Catch potential errors during read_json or to_datetime
            print(f"Error processing JSON/Timestamp data into DataFrame: {e}")
            raise

        if df.empty:
            print("Received empty DataFrame, nothing to load.")
            return

        conn = None # Initialize conn to None
        try:
            conn = snowflake.connector.connect(
                user=SNOWFLAKE_USER,
                password=SNOWFLAKE_PASSWORD,
                account=SNOWFLAKE_ACCOUNT,
                warehouse=SNOWFLAKE_WAREHOUSE,
                database=SNOWFLAKE_DATABASE,
                schema=SNOWFLAKE_SCHEMA
            )
            print("Successfully connected to Snowflake.")

            from snowflake.connector.pandas_tools import write_pandas

            # Ensure column names are uppercase for Snowflake best practices
            df.columns = [col.upper() for col in df.columns]

            print(f"Attempting to load data into table: {SNOWFLAKE_TABLE.upper()}")
            # write_pandas should handle the Pandas datetime column correctly
            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=df,
                table_name=SNOWFLAKE_TABLE.upper(),
                auto_create_table=False, # Table should already exist
                overwrite=False          # Append data
            )
            if success:
                print(f"Successfully loaded {nrows} rows into Snowflake in {nchunks} chunks.")
            else:
                 # Although write_pandas might raise an exception on failure, add explicit check
                 raise RuntimeError("Snowflake write_pandas reported failure but did not raise an exception.")

        except snowflake.connector.Error as sf_err:
             print(f"Snowflake Connector Error: {sf_err}")
             # You might want specific handling for different Snowflake errors
             raise
        except Exception as e:
            print(f"An unexpected error occurred during Snowflake load: {e}")
            raise
        finally:
            if conn:
                conn.close()
                print("Snowflake connection closed.")

    # --- TASK DEPENDENCIES ---
    # Defines the order: extract -> transform -> load
    raw_data = extract()
    clean_data = transform(raw_data)
    load(clean_data)

# This line is essential for Airflow to discover the DAG
weather_etl_dag()

