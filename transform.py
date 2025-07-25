import pandas as pd
from datetime import datetime
import logging

class WeatherTransformer:
    def __init__(self):
        pass
    
    def transform_weather_data(self, raw_data):
        """Transform raw API data into structured format"""
        transformed_data = []
        
        for data in raw_data:
            try:
                timestamp = data['dt']
                if isinstance(timestamp, (int, float)):
                    dt_object = datetime.fromtimestamp(timestamp)
                    formatted_datetime = dt_object.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    formatted_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                transformed_record = {
                    'city': data['name'],
                    'country': data['sys']['country'],
                    'temperature': data['main']['temp'],
                    'feels_like': data['main']['feels_like'],
                    'humidity': data['main']['humidity'],
                    'pressure': data['main']['pressure'],
                    'weather_main': data['weather'][0]['main'],
                    'weather_description': data['weather'][0]['description'],
                    'wind_speed': data.get('wind', {}).get('speed', 0),
                    'wind_direction': data.get('wind', {}).get('deg', 0),
                    'visibility': data.get('visibility', 0),
                    'data_timestamp': formatted_datetime 
                }
                
                transformed_data.append(transformed_record)
                
            except KeyError as e:
                logging.error(f"Error transforming data: {e}")
                continue
            except (ValueError, OSError) as e:
                logging.error(f"Error converting timestamp: {e}")
                # Fallback to current time
                transformed_record['data_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                transformed_data.append(transformed_record)
        
        return pd.DataFrame(transformed_data)
    
    def validate_data(self, df):
        """Validate and clean the transformed data"""
        df = df.drop_duplicates(subset=['city', 'data_timestamp'])
        
        df['wind_speed'] = df['wind_speed'].fillna(0)
        df['wind_direction'] = df['wind_direction'].fillna(0)
        df['visibility'] = df['visibility'].fillna(0)
        
        df = df[df['temperature'] > -100]
        df = df[df['humidity'].between(0, 100)]
        
        return df
