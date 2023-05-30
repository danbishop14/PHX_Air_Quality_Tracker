import os
import pandas as pd
import snowflake.connector
import seaborn as sns
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from datetime import datetime
from datetime import date, timedelta

# Load environment variables
load_dotenv()

def connect_to_snowflake():
    """
    Establish connection with Snowflake database.
    
    Returns:
        snowflake.connector.connection object
    """
    try:
        return snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA')
        )
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        return None

def fetch_data(conn):
    """
    Fetch data from Snowflake database.
    
    Parameters:
        conn (snowflake.connector.connection): Connection object for Snowflake.
        
    Returns:
        df (pandas.DataFrame): Dataframe containing fetched data.
    """
    try:
        query = "SELECT * FROM AIR_QUALITY_DATA"
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=[x[0] for x in cursor.description])
        df = df.sort_values('RECORD_TIMESTAMP', ascending=False)
        df = df.drop_duplicates(subset=['DATE'], keep='first')
        return df
    except Exception as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame()

pollutant_data = {
    'AQI': {
        'Pollutant': 'Air Quality Index',
        'Health Effects': 'The effects of air quality can range from minor respiratory discomfort to more serious conditions such as lung cancer and cardiovascular problems.',
        'Groups Most at Risk': 'All demographic groups are susceptible to the effects of poor air quality. However, individuals with pre-existing health conditions, the elderly, and children are particularly vulnerable when AQI levels rise.',
        'Common Sources': 'The AQI is not a source of pollution itself, rather it is an index that measures and quantifies the overall quality of air by considering various pollutants.'
    },
    'PM10': {
        'Pollutant': 'Particulate Matter ≤ 10 microns',
        'Health Effects': 'Exposure to PM10 can exacerbate existing heart or lung diseases, and can lead to premature death. PM10 can penetrate the lungs and, due to their small size, may even enter the bloodstream.',
        'Groups Most at Risk': 'Individuals with pre-existing heart or lung diseases, children, and the elderly are most susceptible to the adverse health effects of PM10.',
        'Common Sources': 'PM10 primarily originates from crushing or grinding operations and dust that is stirred up by vehicles on roads. In regions like Phoenix, additional sources include dust storms and extensive construction activities.'
    },
    'O3': {
        'Pollutant': 'Ozone',
        'Health Effects': 'Ozone in the lower atmosphere can cause inflammation of the lungs, decrease lung function, and exacerbate respiratory conditions such as asthma and chronic bronchitis.',
        'Groups Most at Risk': 'Children, people suffering from asthma or other respiratory diseases, and outdoor workers are most at risk from ozone pollution.',
        'Common Sources': 'Ground-level ozone is produced from chemical reactions between sunlight, nitrogen oxides (NOx), and volatile organic compounds (VOCs). The sunny weather in Phoenix can accelerate these reactions, potentially leading to higher ozone levels.'
    },
    'PM2_5': {
        'Pollutant': 'Particulate Matter ≤ 2.5 microns',
        'Health Effects': 'PM2.5 can cause a range of respiratory and cardiovascular issues, as well as premature death. These fine particles can penetrate deep into the lungs and bloodstream, causing inflammation and exacerbating pre-existing health conditions.',
        'Groups Most at Risk': 'Individuals with heart or lung diseases, children, and the elderly are most susceptible to PM2.5. Prolonged exposure can lead to serious health complications.',
        'Common Sources': 'PM2.5 particles can originate from various sources including vehicles, power plants, wood burning, and certain industrial processes. These particles can also form from chemical reactions in the atmosphere. In cities like Phoenix, vehicle emissions are a major contributor to PM2.5 levels.'
    }
                }

def plot_air_quality_metrics(df):
    metrics = ['AQI', 'PM10', 'O3', 'PM2_5']

    df_plot = (
        df.assign(
            HOUR=lambda df: pd.to_datetime(df['DATE']).dt.hour,
            DATE=lambda df: pd.to_datetime(df['DATE']).dt.date))

    df_plot_all = df_plot.copy()  # Preserve the original dataframe for weekly average calculation

    # Get the unique dates
    unique_dates = sorted(df_plot['DATE'].unique(), reverse=True)[:5]
    today_date = pd.to_datetime("today").date()

    # Prepare the options for the select box
    unique_dates_str = []
    for date in unique_dates:
        date_str = date.strftime('%A, %m/%d/%Y')
        if date == today_date:
            date_str += " (Today)"
        unique_dates_str.append(date_str)

    default_index = unique_dates_str.index(next(date_str for date_str in unique_dates_str if "Today" in date_str))
    selected_date_str = st.selectbox('Select a day to view forecast', options=unique_dates_str, index=default_index)

    df_plot['DATE'] = pd.to_datetime(df_plot['DATE'])
    df_plot_all['DATE'] = pd.to_datetime(df_plot_all['DATE'])



    # Parse the selected date string to get the date
    selected_date = pd.to_datetime(datetime.strptime(selected_date_str.split(",")[1].strip().split(" ")[0], '%m/%d/%Y'))

    # Then compare
    df_plot = df_plot[df_plot['DATE'].dt.normalize() == selected_date.normalize()]




    if not df_plot.empty:
        for metric in metrics:
            col1, col2 = st.columns([4, 3])

            with col1:
                subcol1, subcol2 = col1.columns([1,1])
                subcol1.subheader(metric)

                # Get the short description of the metric
                metric_description = pollutant_data.get(metric, {}).get('Pollutant', '')

                # Display the short description
                if metric_description:
                    subcol1.write(metric_description)

                avg_24hr = df_plot[metric].mean()

                # Calculate the weekly average and the percent change
                weekly_avg = df_plot_all[df_plot_all['DATE'].dt.date.between((selected_date - pd.DateOffset(weeks=1)).date(), (selected_date - pd.DateOffset(days=1)).date())][metric].mean()

                percent_change = (avg_24hr - weekly_avg) / weekly_avg * 100 if weekly_avg else 0

                units = {'AQI': '', 'PM10': 'µg/m³', 'O3': 'ppb', 'PM2_5': 'µg/m³'}

                # Use st.metric to present the current and 24hr averages with delta as percent change
                subcol2.metric(label="Day's forecast", value=f"{avg_24hr:.2f} {units[metric]}", delta=f"{percent_change:.2f}% \nfrom last week", delta_color="off")

                                
                # Display pollutant information for the metric
                pollutant_info = pollutant_data.get(metric)
                if pollutant_info:
                    st.write("### Pollutant Information")
                    st.markdown("**Health Effects**: " + pollutant_info['Health Effects'])
                    st.markdown("**Groups Most at Risk**: " + pollutant_info['Groups Most at Risk'])
                    st.markdown("**Common Sources**: " + pollutant_info['Common Sources'])

                units = {'AQI': '', 'PM10': 'µg/m³', 'O3': 'ppb', 'PM2_5': 'µg/m³'}
                custom_color_scale = ["green", "yellow",'orange', "red", "purple"]

            with col2:
                fig = px.bar(df_plot, x='HOUR', y=metric, color=metric,
                            color_continuous_scale=custom_color_scale)

                # Customizing layout and axis titles
                fig.update_layout(
                    title=f"{metric} distribution over 24 hours",
                    xaxis_title="Hour of the day",
                    yaxis_title=f"{metric} ({units[metric]})",
                    plot_bgcolor='rgba(0, 0, 0, 0)',  # Making the background transparent
                    font=dict(
                        family="Courier New, monospace",
                        size=12,
                        color="#7f7f7f"
                    )
                )
                            
                fig.update_xaxes(
                    ticktext=['12 AM', '2 AM', '4 AM', '6 AM', '8 AM', '10 AM', '12 PM', '2 PM', '4 PM', '6 PM', '8 PM', '10 PM'],
                    tickvals=[0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22],
                    tickangle=-45,
                    showgrid=False,  # Removing the gridlines
                    zeroline=False,  # Removing the x=0 line
                )
                            
                fig.update_yaxes(
                    showgrid=False,  # Removing the gridlines
                    zeroline=False,  # Removing the y=0 line
                )

                st.plotly_chart(fig)
            st.write('---')

    else:
        st.write("No data available for the selected date.")


def calculate_and_display_summary(df):
    # Convert DATE to datetime if it isn't already
    df['DATE'] = pd.to_datetime(df['DATE'])
    # Make sure data is sorted by date in descending order
    df = df.sort_values('DATE', ascending=False)

    # Extract the most recent 5 days
    last_five_days = df['DATE'].dt.date.unique()[:5]

    # Create a summary dataframe with daily averages for each metric
    summary_df = pd.DataFrame()

    for day in last_five_days:
        # Filter for the specific day
        df_day = df[df['DATE'].dt.date == day]
        # Calculate the average
        daily_avg = df_day[['AQI','O3', 'PM10', 'PM2_5']].mean()
        
        # Create a custom column name with the day of the week and the date
        col_name = pd.MultiIndex.from_tuples([(day.strftime('%A'), day.strftime('%m/%d/%Y'))], names=['Day', 'Date'])
        
        # Add to the summary dataframe with the custom column name
        summary_df = pd.concat([summary_df, pd.DataFrame(daily_avg).T.set_index(col_name)], axis=0)

    # Transpose the dataframe to get the desired format
    summary_df = summary_df.transpose().applymap("{0:.2f}".format)
    # Reverse the order of the columns
    summary_df = summary_df[summary_df.columns[::-1]]

    return summary_df

def display_worst_day_warning(df,pollutant_data):
    # Get the next five days
    today = date.today()
    next_five_days = [today + timedelta(days=i) for i in range(1, 6)]

    # Filter the dataframe for the next five days
    df_next_five_days = df[df['DATE'].dt.date.isin(next_five_days)]

    # Calculate the average for each day and metric
    daily_avg = df_next_five_days.groupby(df_next_five_days['DATE'].dt.date)[['AQI', 'PM10', 'O3', 'PM2_5']].mean()

    # Find the day with the maximum average for each metric
    worst_days = daily_avg.idxmax()

    st.markdown('Please note the following dates observed to have **worst air quality conditions** for specific pollutants:')
    for metric, day in worst_days.items():
        st.markdown(f"**{pollutant_data[metric]['Pollutant']}**: {day.strftime('%A, %m/%d/%Y')}")


# Setting the Streamlit page configuration
st.set_page_config(page_title="Air Quality Forecast", page_icon=":robot:",layout='centered')
st.write("# Phoenix Air Quality Forecast")

if "generated_blog" not in st.session_state:
    st.session_state.generated_blog = ""

def main():
    conn = connect_to_snowflake()
    df = fetch_data(conn)
    timestamp = (df['RECORD_TIMESTAMP'].max()).strftime('%I:%M%p')
    st.warning(f'Anticipate the effects of air pollutants on allergies, respiratory conditions, and overall health. Harness real-time air quality information to protect your well-being and plan your activities in Phoenix, Arizona.  \n\n Powered by [OpenWeatherMap](https://openweathermap.org/), [Snowflake](https://www.snowflake.com/en/), and [Streamlit](https://www.streamlit.com/).')
    st.write(f"Data updated: **{timestamp}**")

    summary_df = calculate_and_display_summary(df)
    st.write("## The Week Ahead:")
    col1, col2 = st.columns([6,3])
    with col1:
        # Display the summary table
        st.dataframe(summary_df)
        st.write("O3 = Ozone, PM10 = Particles ≤ 10 microns, PM2.5 = Particles ≤ 2.5 microns")
    with col2:
        # Display the worst day warning
        display_worst_day_warning(df, pollutant_data)

    st.write('---')
    st.write("## View Daily Forecasts")
    plot_air_quality_metrics(df)

if __name__ == "__main__":
    main()

