import os
import streamlit as st

try:
    
    DB_CONFIG = {
        'host': st.secrets["database"]["DB_HOST"],
        'user': st.secrets["database"]["DB_USER"],
        'password': st.secrets["database"]["DB_PASSWORD"],
        'database': st.secrets["database"]["DB_NAME"],
        'port': int(st.secrets["database"]["DB_PORT"])
    }
    OPENWEATHER_API_KEY = st.secrets["api"]["OPENWEATHER_API_KEY"]
    OPENWEATHER_BASE_URL = 'https://api.openweathermap.org/data/2.5/weather'
    
except:
    from dotenv import load_dotenv
    load_dotenv()
    
    DB_CONFIG = {
        'host': os.getenv('DB_HOST', 'sql12.freesqldatabase.com'),
        'user': os.getenv('DB_USER', 'sql12792014'),
        'password': os.getenv('DB_PASSWORD', 'your-actual-password'),
        'database': os.getenv('DB_NAME', 'sql12792014'),
        'port': int(os.getenv('DB_PORT', 3306))
    }
    OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY')
    OPENWEATHER_BASE_URL = 'https://api.openweathermap.org/data/2.5/weather'
CITIES = ['London', 'New York', 'Tokyo', 'Mumbai', 'Sydney']
