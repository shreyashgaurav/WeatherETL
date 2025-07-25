CREATE DATABASE weather_analytics;

USE weather_analytics;

CREATE TABLE weather_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    country VARCHAR(50),
    temperature DECIMAL(5,2),
    feels_like DECIMAL(5,2),
    humidity INT,
    pressure INT,
    weather_main VARCHAR(50),
    weather_description VARCHAR(100),
    wind_speed DECIMAL(5,2),
    wind_direction INT,
    visibility INT,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_timestamp DATETIME
);
