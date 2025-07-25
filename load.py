import mysql.connector
import pandas as pd
import logging
from config import DB_CONFIG

class WeatherLoader:
    def __init__(self):
        self.db_config = DB_CONFIG
    
    def get_connection(self):
        """Create database connection"""
        try:
            connection = mysql.connector.connect(**self.db_config)
            return connection
        except mysql.connector.Error as e:
            logging.error(f"Database connection error: {e}")
            return None
    
    def load_data(self, df):
        """Load transformed data into MySQL database"""
        connection = self.get_connection()
        if not connection:
            return False
        
        try:
            cursor = connection.cursor()
            
            # Prepare insert query
            insert_query = """
                INSERT INTO weather_data 
                (city, country, temperature, feels_like, humidity, pressure, 
                 weather_main, weather_description, wind_speed, wind_direction, 
                 visibility, data_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            # Convert DataFrame to list of tuples
            data_tuples = df.to_records(index=False).tolist()
            
            # Execute batch insert
            cursor.executemany(insert_query, data_tuples)
            connection.commit()
            
            logging.info(f"Successfully loaded {len(data_tuples)} records")
            return True
            
        except mysql.connector.Error as e:
            logging.error(f"Error loading data: {e}")
            connection.rollback()
            return False
            
        finally:
            cursor.close()
            connection.close()
