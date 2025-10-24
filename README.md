Automated Weather Data ETL Pipeline with Airflow and Snowflake

Project Overview

This project demonstrates a complete Extract, Transform, Load (ETL) pipeline built to automatically fetch real-time weather data for selected cities, clean it, and load it into a Snowflake cloud data warehouse. The entire workflow is orchestrated using Apache Airflow.

Technologies Used

Python: Core programming language for scripting.

Requests: For fetching data from the OpenWeatherMap API.

Pandas: For data transformation and structuring.

Snowflake: Cloud data warehouse for final data storage.

snowflake-connector-python: To connect Python scripts to Snowflake.

Apache Airflow: For workflow automation, scheduling, and monitoring.

WSL (Ubuntu): Linux environment for running Apache Airflow.

Git & GitHub: For version control and code sharing.

How it Works

The pipeline consists of three main tasks orchestrated by Airflow:

Extract: A Python function (extract) uses the OpenWeatherMap API to fetch current weather data (temperature, humidity, description) for predefined cities (Chennai, Mumbai, Delhi, Bangalore, Tiruchirappalli). The raw JSON data is passed to the next task.

Transform: A Python function (transform) receives the raw JSON data, parses it, cleans it (e.g., converts temperature from Kelvin to Celsius), adds a UTC timestamp, and structures it into a Pandas DataFrame. The cleaned DataFrame is then passed to the next task.

Load: A Python function (load) connects to the specified Snowflake database, takes the cleaned Pandas DataFrame, and appends the data into the DAILY_WEATHER table using the write_pandas utility.

The entire process is defined as an Airflow DAG (weather_etl_pipeline) scheduled to run daily (@daily).

Airflow Orchestration

The pipeline is managed via the Apache Airflow UI.

(Airflow UI showing the weather_etl_pipeline DAG with successful task runs)

Snowflake Data Warehouse

The final, clean data resides in the DAILY_WEATHER table within Snowflake.

(Snowflake worksheet displaying the data loaded into the DAILY_WEATHER table)

Setup

Prerequisites: Python 3.x, pip, Git.

Clone Repository: git clone <your-repository-url>

Install Dependencies:

Set up WSL (Ubuntu) if on Windows.

Create and activate a Python virtual environment:

python3 -m venv venv
source venv/bin/activate


Install required libraries:

pip install apache-airflow requests pandas "snowflake-connector-python[pandas]"


Configure Credentials: Update the placeholder values in the CONFIGURATION section of weather_pipeline.py with your OpenWeatherMap API key and Snowflake account details.

Snowflake Setup: Ensure the target database (WEATHER_DB), schema (PIPELINE), and table (DAILY_WEATHER) exist in your Snowflake account with the correct column structure.

Airflow Setup:

Initialize Airflow (e.g., airflow standalone).

Copy weather_pipeline.py to the ~/airflow/dags/ folder in your Airflow environment.

Enable and trigger the weather_etl_pipeline DAG from the Airflow UI (http://localhost:8080).

Future Improvements

Implement more robust error handling and alerting.

Add data quality checks.

Parameterize city list or Snowflake connection details.

Store raw data in a staging area (like AWS S3) before transformation.
