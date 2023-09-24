# Real-time Weather Data Pipeline

## Description

The Real-time Weather Data Pipeline is a simple data processing application that collects and analyses weather data from 60 cities around the world in real-time. 

It leverages Apache Kafka for data ingestion and Spark for data processing and analytics. This pipeline provides a continuous stream of weather information, enabling users to monitor and analyse weather patterns across multiple locations simultaneously.

## Key components

### Data Ingestion
The application retrieves weather data from multiple cities using the OpenWeatherMap API. It periodically fetches geolocation and weather information for a list of 60 cities.

### Apache Kafka
Kafka serves as the data streaming platform. A Kafka producer component is responsible for fetching weather data and sending it to Kafka topics. It collects data for multiple cities and batches it before transmitting.

### Apache Spark Structured Streaming
Spark Structured Streaming is used for real-time data processing. It consumes weather data from Kafka topics, and computes various statistics, such as average temperature, wind speed, humidity, and pressure.

## Installation

### Prerequisites
- **Java Development Kit** (JDK) installed on your machine.
- Download and extract **Kafka**: [https://kafka.apache.org/downloads](https://kafka.apache.org/downloads)

### Run the app
- Start a Zookeeper instance:
```
cd /path/to/kafka
bin/zookeeper-server-start.sh config/zookeeper.properties
```

- Start Kafka Broker:
```
bin/kafka-server-start.sh config/server.properties
```

- Install dependencies and run the app:
```
pip install -r requirements.txt
python main.py
python weather_data_streaming.py
```