import logging
import schedule
import time
from extract import WeatherExtractor
from transform import WeatherTransformer
from load import WeatherLoader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('weather_etl.log'),
        logging.StreamHandler()
    ]
)

class WeatherETLPipeline:
    def __init__(self):
        self.extractor = WeatherExtractor()
        self.transformer = WeatherTransformer()
        self.loader = WeatherLoader()
    
    def run_pipeline(self):
        """Execute the complete ETL pipeline"""
        logging.info("Starting ETL pipeline...")
        
        try:
            # Extract
            logging.info("Extracting weather data...")
            raw_data = self.extractor.fetch_all_cities()
            
            if not raw_data:
                logging.warning("No data extracted")
                return
            
            # Transform
            logging.info("Transforming data...")
            df = self.transformer.transform_weather_data(raw_data)
            df = self.transformer.validate_data(df)
            
            if df.empty:
                logging.warning("No valid data after transformation")
                return
            
            # Load
            logging.info("Loading data into database...")
            success = self.loader.load_data(df)
            
            if success:
                logging.info("ETL pipeline completed successfully")
            else:
                logging.error("ETL pipeline failed during loading")
                
        except Exception as e:
            logging.error(f"ETL pipeline failed: {e}")

def main():
    pipeline = WeatherETLPipeline()
    
    
    pipeline.run_pipeline()
    
    # Scheduling to run every hour
    schedule.every().hour.do(pipeline.run_pipeline)
    
    # Keep the script running
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()
