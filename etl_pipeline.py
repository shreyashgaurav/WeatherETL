import logging
import schedule
import time
import sys
from datetime import datetime
from extract import WeatherExtractor
from transform import WeatherTransformer
from load import WeatherLoader

# Configure logging with rotation
from logging.handlers import RotatingFileHandler

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Rotating file handler (max 10MB, keep 5 files)
file_handler = RotatingFileHandler('weather_etl.log', maxBytes=10*1024*1024, backupCount=5)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# Console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

logger.addHandler(file_handler)
logger.addHandler(console_handler)

class WeatherETLPipeline:
    def __init__(self):
        self.extractor = WeatherExtractor()
        self.transformer = WeatherTransformer()
        self.loader = WeatherLoader()
        self.failure_count = 0
        self.max_failures = 5
    
    def run_pipeline(self):
        """Execute the complete ETL pipeline with error recovery"""
        logging.info(f"Starting ETL pipeline at {datetime.now()}")
        
        try:
            # Extract
            logging.info("Extracting weather data...")
            raw_data = self.extractor.fetch_all_cities()
            
            if not raw_data:
                logging.warning("No data extracted")
                self.failure_count += 1
                return
            
            # Transform
            logging.info("Transforming data...")
            df = self.transformer.transform_weather_data(raw_data)
            df = self.transformer.validate_data(df)
            
            if df.empty:
                logging.warning("No valid data after transformation")
                self.failure_count += 1
                return
            
            # Load
            logging.info("Loading data into database...")
            success = self.loader.load_data(df)
            
            if success:
                logging.info(f"ETL pipeline completed successfully. Processed {len(df)} records.")
                self.failure_count = 0  # Reset failure count on success
            else:
                logging.error("ETL pipeline failed during loading")
                self.failure_count += 1
                
        except Exception as e:
            logging.error(f"ETL pipeline failed: {e}")
            self.failure_count += 1
        
        # Check if too many failures
        if self.failure_count >= self.max_failures:
            logging.critical(f"Too many failures ({self.failure_count}). Stopping pipeline.")
            sys.exit(1)

def main():
    pipeline = WeatherETLPipeline()
    
    # Run immediately
    pipeline.run_pipeline()
    
    # Schedule to run every hour
    schedule.every().hour.do(pipeline.run_pipeline)
    
    logging.info("ETL pipeline started. Scheduled to run every hour.")
    logging.info("Press Ctrl+C to stop gracefully.")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)
            
            # Heartbeat log every hour
            if datetime.now().minute == 0:
                logging.info(f"ETL scheduler heartbeat: {datetime.now()}")
                
    except KeyboardInterrupt:
        logging.info("ETL pipeline stopped by user")
    except Exception as e:
        logging.critical(f"ETL scheduler crashed: {e}")
        raise

if __name__ == "__main__":
    main()
