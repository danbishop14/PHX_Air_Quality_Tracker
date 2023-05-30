# Phoenix Air Quality Tracker
The Phoenix Air Quality Forecast app is built using Streamlit, an open-source Python framework for building interactive web applications. The app fetches weather and air quality data from the OpenWeatherMap API and visualizes it to provide users with real-time air quality information.

https://phxairqualityforecast.streamlit.app/

![image](https://github.com/danbishop14/PHX_Air_Quality_Tracker/assets/69700884/b19dddd6-ff83-4127-b1d3-97fa41fccaac)


## Key Features
- Real-time air quality information for Phoenix, Arizona
- Visualizations of air quality metrics
- Daily forecasts and summaries
- Identification of days with the worst air quality conditions

## Technologies Used

- Python
- Streamlit
- OpenWeatherMap API
- Snowflake

## Installation

1. Clone the GitHub repository:
   ```shell
   git clone https://github.com/your-username/phoenix-air-quality-forecast.git
   
2. Navigate to the project directory:
    ```shell
    cd phoenix-air-quality-forecast
4. Install the dependencies::
    ```shell
   pip install -r requirements.txt
5. Set up the necessary environment variables::
    ```shell
    API_KEY: Your OpenWeatherMap API key
    SNOWFLAKE_USER: Your Snowflake database username
    SNOWFLAKE_PASSWORD: Your Snowflake database password
    SNOWFLAKE_ACCOUNT: Your Snowflake account URL
    SNOWFLAKE_DATABASE: Name of the Snowflake database
    SNOWFLAKE_SCHEMA: Name of the Snowflake schema
    
6. Run the ETL script to retrieve and load the initial data::
    ```shell
   python ETL.py
7. Launch the Streamlit app::
    ```shell
   streamlit run Home.py
8. The app will be accessible in your browser at http://localhost:8501.

## Contributing

Contributions are welcome! If you find any issues or have suggestions for improvements, please open an issue or submit a pull request.

## License

This project is licensed under the MIT License.

## Acknowledgments

- This app is powered by OpenWeatherMap, Snowflake, and Streamlit.
- The project structure and codebase were inspired by best practices and examples from the Streamlit community.

Enjoy using the Phoenix Air Quality Forecast app to stay informed about air quality conditions and protect your health!
