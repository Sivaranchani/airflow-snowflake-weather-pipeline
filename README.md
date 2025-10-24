🌦️ Automated Weather Data ETL Pipeline with Airflow and Snowflake ❄️

🎯 Project Overview

This project demonstrates a complete Extract, Transform, Load (ETL) pipeline built to automatically fetch real-time weather data for selected cities, clean it, and load it into a Snowflake cloud data warehouse. The entire workflow is orchestrated using Apache Airflow.

🛠️ Technologies Used

Core: Python, Pandas, Snowflake, Apache Airflow

Data Source: OpenWeatherMap API

Connectivity: Requests, snowflake-connector-python

Environment: WSL (Ubuntu), Python Virtual Environment

Version Control: Git, GitHub

⚙️ How it Works

The pipeline consists of three main tasks orchestrated by Airflow:

Extract:

Fetches current weather data (temperature, humidity, description) via OpenWeatherMap API using requests.

Handles API responses and basic error checking.

Passes raw data as a JSON string to the next task via Airflow XComs.

Transform :

Receives the JSON string, parses it back into Python objects.

Cleans the data (e.g., converts temperature Kelvin -> Celsius).

Adds a UTC timestamp using Python's datetime.

Structures the data into a Pandas DataFrame.

Passes the cleaned DataFrame as a JSON string via Airflow XComs.

Load :

Receives the cleaned data JSON string, converts it back to a Pandas DataFrame.

Connects to the specified Snowflake database using snowflake-connector-python.

Appends the DataFrame to the target DAILY_WEATHER table using write_pandas.

Includes error handling for the database connection and loading process.

The entire process is defined as an Airflow DAG (weather_etl_pipeline) scheduled to run daily (@daily).

📊 Airflow Orchestration

The pipeline's execution, scheduling, and monitoring are managed via the Apache Airflow UI.

<img width="1920" height="1080" alt="Screenshot 2025-10-24 094425" src="https://github.com/user-attachments/assets/766a2cce-6449-41d6-aa36-7dabeb1f3a36" />


💾 Snowflake Data Warehouse

The final, clean, and structured weather data is stored in the DAILY_WEATHER table within the specified Snowflake database and schema.

<img width="1483" height="348" alt="Screenshot 2025-10-24 094611" src="https://github.com/user-attachments/assets/1201b77f-9635-41b0-990d-c94bae126434" />


🚀 Setup & Installation

Prerequisites: Python 3.x, pip, Git.

Snowflake Setup:

Log in to your Snowflake account.

Ensure the target database (WEATHER_DB), schema (PIPELINE), and warehouse (COMPUTE_WH) exist.

Create the target table if it doesn't exist:

CREATE TABLE WEATHER_DB.PIPELINE.DAILY_WEATHER (
    City VARCHAR,
    Temperature_C FLOAT,
    Humidity_Percent INT,
    Description VARCHAR,
    Timestamp_UTC TIMESTAMP_NTZ -- Ensure this matches the DataFrame
);


Airflow Setup:

Initialize Airflow (e.g., using airflow standalone in your activated virtual environment).

Copy the configured weather_pipeline.py to the ~/airflow/dags/ folder in your Airflow environment (WSL).

cp weather_pipeline.py ~/airflow/dags/


Start/Restart airflow standalone.

Access the Airflow UI (http://localhost:8080), find the weather_etl_pipeline DAG, unpause it (toggle switch), and trigger it manually (play button).

✨ Future Improvements

💡 Implement more detailed logging and monitoring within Airflow tasks.

💡 Add notifications (e.g., email/Slack) for pipeline failures.

💡 Incorporate data quality checks (e.g., using Great Expectations).

💡 Parameterize configurations (cities, Snowflake details) using Airflow Variables or Connections.

💡Introduce a staging layer (e.g., AWS S3) for raw data before transformation (ELT pattern).

💡 Add unit/integration tests for pipeline tasks.
