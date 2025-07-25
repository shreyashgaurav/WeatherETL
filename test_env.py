import os
from dotenv import load_dotenv

load_dotenv()

print("Testing environment variables:")
print(f"API Key: {os.getenv('OPENWEATHER_API_KEY')[:10]}...")  # Show only first 10 chars
print(f"DB Host: {os.getenv('DB_HOST')}")
print(f"DB User: {os.getenv('DB_USER')}")
print(f"DB Name: {os.getenv('DB_NAME')}")
print(f"DB Password: {'*' * len(os.getenv('DB_PASSWORD', ''))}")  # Hide password
