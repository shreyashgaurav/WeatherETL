import requests
import logging
from config import OPENWEATHER_API_KEY, OPENWEATHER_BASE_URL, CITIES

class WeatherExtractor:
    def __init__(self):
        self.api_key = OPENWEATHER_API_KEY
        self.base_url = OPENWEATHER_BASE_URL
        
    def fetch_weather_data(self, city):
        """Fetch weather data for a specific city"""
        try:
            params = {
                'q': city,
                'appid': self.api_key,
                'units': 'metric'
            }
            
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching data for {city}: {e}")
            return None
    
    def fetch_all_cities(self):
        """Fetch weather data for all configured cities"""
        weather_data = []
        
        for city in CITIES:
            data = self.fetch_weather_data(city)
            if data:
                weather_data.append(data)
                
        return weather_data
