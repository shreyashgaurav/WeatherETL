import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import mysql.connector
from datetime import datetime, timedelta
import time
from config import DB_CONFIG

# Configure Streamlit page
st.set_page_config(
    page_title="Weather Analytics Dashboard",
    page_icon="🌤️",
    layout="wide",
    initial_sidebar_state="expanded"
)

class WeatherDashboard:
    def __init__(self):
        self.db_config = DB_CONFIG
    
    def get_connection(self):
        """Create database connection"""
        try:
            return mysql.connector.connect(**self.db_config)
        except mysql.connector.Error as e:
            st.error(f"Database connection error: {e}")
            return None
    
    def fetch_data(self, query, params=None):
        """Execute query and return DataFrame"""
        connection = self.get_connection()
        if not connection:
            return pd.DataFrame()
        
        try:
            df = pd.read_sql(query, connection, params=params)
            return df
        except Exception as e:
            st.error(f"Error fetching data: {e}")
            return pd.DataFrame()
        finally:
            connection.close()
    
    def get_latest_data(self):
        """Get the most recent weather data for all cities"""
        query = """
        SELECT DISTINCT city, country, temperature, feels_like, humidity, 
               pressure, weather_main, weather_description, wind_speed, 
               wind_direction, visibility, data_timestamp
        FROM weather_data w1
        WHERE data_timestamp = (
            SELECT MAX(data_timestamp) 
            FROM weather_data w2 
            WHERE w2.city = w1.city
        )
        ORDER BY city
        """
        return self.fetch_data(query)
    
    def get_historical_data(self, days=7):
        """Get historical data for the last N days"""
        query = """
        SELECT city, temperature, humidity, pressure, data_timestamp
        FROM weather_data 
        WHERE data_timestamp >= DATE_SUB(NOW(), INTERVAL %s DAY)
        ORDER BY data_timestamp DESC
        """
        return self.fetch_data(query, (days,))
    
    def get_city_data(self, city, days=7):
        """Get data for a specific city"""
        query = """
        SELECT * FROM weather_data 
        WHERE city = %s AND data_timestamp >= DATE_SUB(NOW(), INTERVAL %s DAY)
        ORDER BY data_timestamp DESC
        """
        return self.fetch_data(query, (city, days))
    
    def get_analytics_data(self):
        """Get analytics data"""
        queries = {
            'avg_temp': """
                SELECT city, AVG(temperature) as avg_temp, 
                       MIN(temperature) as min_temp, MAX(temperature) as max_temp
                FROM weather_data 
                WHERE data_timestamp >= DATE_SUB(NOW(), INTERVAL 7 DAY)
                GROUP BY city
                ORDER BY avg_temp DESC
            """,
            'weather_distribution': """
                SELECT weather_main, COUNT(*) as count
                FROM weather_data 
                WHERE data_timestamp >= DATE_SUB(NOW(), INTERVAL 7 DAY)
                GROUP BY weather_main
                ORDER BY count DESC
            """,
            'hourly_trends': """
                SELECT HOUR(data_timestamp) as hour, 
                       AVG(temperature) as avg_temp,
                       AVG(humidity) as avg_humidity
                FROM weather_data 
                WHERE data_timestamp >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
                GROUP BY HOUR(data_timestamp)
                ORDER BY hour
            """
        }
        
        return {key: self.fetch_data(query) for key, query in queries.items()}

def main():
    # Initialize dashboard
    dashboard = WeatherDashboard()
    
    # Header
    st.title("🌤️ Weather Analytics Dashboard")
    st.markdown("---")
    
    # Sidebar
    st.sidebar.header("🎛️ Controls")
    
    # Auto-refresh option
    auto_refresh = st.sidebar.checkbox("🔄 Auto Refresh (30s)")
    if auto_refresh:
        time.sleep(30)
        st.rerun()
    
    # Manual refresh button
    if st.sidebar.button("🔄 Refresh Data"):
        st.rerun()
    
    # Date range selector
    days_range = st.sidebar.selectbox(
        "📅 Data Range",
        [1, 3, 7, 14, 30],
        index=2,
        format_func=lambda x: f"Last {x} day{'s' if x > 1 else ''}"
    )
    
    # Get data
    latest_data = dashboard.get_latest_data()
    historical_data = dashboard.get_historical_data(days_range)
    analytics_data = dashboard.get_analytics_data()
    
    if latest_data.empty:
        st.error("❌ No data available. Make sure your ETL pipeline is running!")
        return
    
    # Main content
    col1, col2, col3, col4 = st.columns(4)
    
    # Key Metrics
    with col1:
        avg_temp = latest_data['temperature'].mean()
        st.metric("🌡️ Avg Temperature", f"{avg_temp:.1f}°C")
    
    with col2:
        avg_humidity = latest_data['humidity'].mean()
        st.metric("💧 Avg Humidity", f"{avg_humidity:.0f}%")
    
    with col3:
        cities_count = len(latest_data)
        st.metric("🏙️ Cities Tracked", cities_count)
    
    with col4:
        total_records = len(historical_data)
        st.metric("📊 Total Records", total_records)
    
    st.markdown("---")
    
    # Current Weather Cards
    st.subheader("🌍 Current Weather Conditions")
    
    cols = st.columns(min(len(latest_data), 5))
    for idx, (_, row) in enumerate(latest_data.iterrows()):
        if idx < len(cols):
            with cols[idx]:
                # Weather emoji mapping
                weather_emojis = {
                    'Clear': '☀️',
                    'Clouds': '☁️',
                    'Rain': '🌧️',
                    'Snow': '❄️',
                    'Thunderstorm': '⛈️',
                    'Drizzle': '🌦️',
                    'Mist': '🌫️',
                    'Fog': '🌫️'
                }
                
                emoji = weather_emojis.get(row['weather_main'], '🌤️')
                
                st.markdown(f"""
                <div style="
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    padding: 1rem;
                    border-radius: 10px;
                    color: white;
                    text-align: center;
                    margin-bottom: 1rem;
                ">
                    <h3>{emoji} {row['city']}</h3>
                    <h1>{row['temperature']:.1f}°C</h1>
                    <p>Feels like {row['feels_like']:.1f}°C</p>
                    <p>{row['weather_description'].title()}</p>
                    <small>💧 {row['humidity']}% | 💨 {row['wind_speed']:.1f} m/s</small>
                </div>
                """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Charts Section
    chart_col1, chart_col2 = st.columns(2)
    
    with chart_col1:
        st.subheader("📈 Temperature Trends")
        if not historical_data.empty:
            fig = px.line(
                historical_data, 
                x='data_timestamp', 
                y='temperature', 
                color='city',
                title="Temperature Over Time"
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    with chart_col2:
        st.subheader("💧 Humidity vs Temperature")
        if not latest_data.empty:
            fig = px.scatter(
                latest_data, 
                x='temperature', 
                y='humidity',
                size='pressure',
                color='city',
                title="Current Humidity vs Temperature",
                hover_data=['weather_main']
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    # Analytics Section
    st.markdown("---")
    st.subheader("📊 Analytics")
    
    analytics_col1, analytics_col2, analytics_col3 = st.columns(3)
    
    with analytics_col1:
        st.write("**🌡️ Temperature Analytics**")
        if not analytics_data['avg_temp'].empty:
            fig = px.bar(
                analytics_data['avg_temp'], 
                x='city', 
                y='avg_temp',
                title=f"Average Temperature (Last {days_range} days)"
            )
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)
    
    with analytics_col2:
        st.write("**🌤️ Weather Distribution**")
        if not analytics_data['weather_distribution'].empty:
            fig = px.pie(
                analytics_data['weather_distribution'], 
                values='count', 
                names='weather_main',
                title="Weather Conditions Distribution"
            )
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)
    
    with analytics_col3:
        st.write("**⏰ Hourly Trends**")
        if not analytics_data['hourly_trends'].empty:
            fig = px.line(
                analytics_data['hourly_trends'], 
                x='hour', 
                y='avg_temp',
                title="Average Hourly Temperature"
            )
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)
    
    # Detailed Data Table
    st.markdown("---")
    st.subheader("📋 Detailed Data")
    
    # City selector
    selected_city = st.selectbox(
        "Select City for Detailed View:",
        ["All Cities"] + list(latest_data['city'].unique())
    )
    
    if selected_city == "All Cities":
        display_data = historical_data.head(50)
    else:
        display_data = dashboard.get_city_data(selected_city, days_range)
    
    if not display_data.empty:
        st.dataframe(
            display_data[['city', 'temperature', 'humidity', 'pressure', 
                         'weather_main', 'wind_speed', 'data_timestamp']],
            use_container_width=True
        )
    
    # Footer
    st.markdown("---")
    st.markdown(
        f"<div style='text-align: center; color: gray;'>"
        f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
        f"Data range: Last {days_range} days"
        f"</div>", 
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()
