import os
import pandas as pd
import snowflake.connector
import streamlit as st
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

# Connect to Snowflake DB
def connect_to_snowflake():
    return snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )

# Fetch data from Snowflake
def fetch_data(conn):
    query = "SELECT * FROM AIR_QUALITY_DATA"
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=[x[0] for x in cursor.description])
    # Deduplicate records, keep only the most recent record
    df = df.sort_values('RECORD_TIMESTAMP', ascending=False)
    df = df.drop_duplicates(subset=['DATE'], keep='first')
    
    return df

def compute_metrics(df):
    # Compute the size of data in bytes
    size_of_data = df.memory_usage().sum()
    last_run_time = df['RECORD_TIMESTAMP'].max()
    return size_of_data, last_run_time

def format_size(size):
    # size is in bytes
    if size < 1024:
        return f"{size} bytes"
    elif size < 1024 ** 2:
        return f"{size / 1024:.2f} KB"
    elif size < 1024 ** 3:
        return f"{size / (1024 ** 2):.2f} MB"
    else:
        return f"{size / (1024 ** 3):.2f} GB"


def metadata_description():

        st.markdown("""
        This page provides insights into the data collected from the OpenWeatherMap API for weather and air quality data. The data is stored in a Snowflake database, and is updated every 24 hours using a GitHub Actions workflow and a Cron job.

        Key components of the ETL (Extract-Transform-Load) pipeline:

        1. **Extract**: Fetches latitude and longitude for a specific location, and uses these coordinates to retrieve weather and air quality data.
        2. **Transform**: Parses the obtained JSON data to extract useful attributes like temperature, humidity, air quality indices etc.
        3. **Load**: Inserts the transformed data into the respective Snowflake database table (`AIR_QUALITY_DATA`) and includes a timestamp (`RECORD_TIMESTAMP`) indicating the data extraction time.

        The regular updates to the data allow for monitoring trends over time and ensuring data quality.
        """)

def main():
    # Connect to database
    conn = connect_to_snowflake()

    # Fetch data
    df = fetch_data(conn)

    # Compute metrics
    database_size, last_run_time = compute_metrics(df)

    # Compute data points added today
    today = datetime.now().date()
    data_points_added_today = len(df[df['DATE'].dt.date == today])

    # Compute number of days pulling data
    first_data_date = df['DATE'].min().date()
    days_pulling_data = (today - first_data_date).days

    # Compute the delta % changes
    delta_days_pulling_data = (days_pulling_data - (days_pulling_data - 1)) / (days_pulling_data - 1) * 100
    delta_data_points_added_today = (data_points_added_today - (data_points_added_today - 1)) / (data_points_added_today - 1) * 100
    delta_database_size = (database_size - (database_size - 1)) / (database_size - 1) * 100

    # Display metrics on the dashboard
    st.header("ETL Metrics")

    col1, col2, col3, col4 = st.columns([2.5,2,2,2])

    with col1:
        st.metric("Last ETL Run", last_run_time.strftime('%m-%d %H:%M'))

    with col2:
        st.metric("Data Points Added Today", data_points_added_today, f"{delta_data_points_added_today:.2f}% increase")

    with col3:
        st.metric("Database Size", format_size(database_size))

    with col4:
        st.metric("Total Days Running", days_pulling_data)

    # Aggregate data by date and compute cumulative sum
    df['DATE'] = pd.to_datetime(df['DATE']).dt.date
    df_daily = df.groupby('DATE').size().reset_index(name='COUNT')
    df_daily['CUMULATIVE_COUNT'] = df_daily['COUNT'].cumsum()

    # Plot the data
    st.line_chart(df_daily.set_index('DATE')['CUMULATIVE_COUNT'])

    metadata_description()

# Run the main function
if __name__ == '__main__':
    main()

