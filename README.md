# Food Products Clustering

This project implements a K-means clustering model for food products using PySpark and a data mart on Scala Spark. It analyzes nutritional information from the OpenFoodFacts dataset to identify patterns and group similar products together. The project includes integration with MS SQL Server for data storage, a Scala data mart for data processing, and a FastAPI service for API access.

## Project Structure

```
.
├── config.ini                 # Configuration file
├── docker-compose.yml         # Docker Compose configuration
├── .env                       # Environment variables for Docker
├── src/                       # Python source code
│   ├── api/                   # API service
│   ├── data/                  # Data loading and preprocessing
│   ├── features/              # Feature engineering
│   ├── models/                # Machine learning models
│   ├── utils/                 # Utilities
│   ├── visualization/         # Visualization utilities
│   └── main.py                # Main application entry point
├── datamart/                  # Scala data mart
│   ├── src/                   # Scala source code
│   ├── project/               # SBT project configuration
│   └── build.sbt              # SBT build configuration
├── docker/                    # Docker configuration
├── scripts/                   # Scripts for initialization
│   └── mssql/                 # MS SQL Server scripts
├── tests/                     # Unit tests
├── results/                   # Output directory for visualizations and reports
└── model/                     # Directory for saved models
```

## Requirements

- Python 3.12+
- PySpark 4.0.0+
- Scala 2.13+
- SBT 1.9+
- Java 8+
- MS SQL Server
- Docker and Docker Compose

## Configuration

The application is configured using the `config.ini` file. The main configuration sections are:

- `DEFAULT`: General settings like logging level and random seed
- `DATA`: Data loading settings like URL and sample size
- `PREPROCESSING`: Data preprocessing settings like outlier threshold
- `FEATURES`: Feature engineering settings like feature list and scaling method
- `MODEL`: Model settings like number of clusters and iterations
- `SPARK`: Spark configuration settings like memory allocation and parallelism
- `MSSQL`: MS SQL Server connection settings and table names
- `API`: FastAPI service configuration
- `DATAMART`: Data mart configuration settings
- `SAVED_MODEL`: Path to saved model

## Running with Docker

To run the application with Docker:

```bash
# Remove all volumes and containers
docker compose down -v

# Build and start the containers
docker compose up -d
```

This will start:
1. MS SQL Server container
2. Data mart container (Scala)
3. Application container with FastAPI service

The API will be available at http://localhost:8000

## Data Processing Pipeline

1. **ETL Pipeline (Scala Data Mart)**:
   - Load data from OpenFoodFacts dataset (Parquet files)
   - Preprocess data
   - Save to MS SQL Server

2. **Clustering Pipeline (Python)**:
   - Load data from MS SQL Server via data mart
   - Feature engineering and scaling
   - K-means clustering
   - Save results to MS SQL Server via data mart

## Usage Guide

### Complete Pipeline Execution

1. Start all containers:
   ```bash
   docker compose up -d
   ```

2. Run the ETL pipeline to process data with Scala data mart:
   ```bash
   docker compose run --rm datamart spark-submit --class ru.itmo.datamart.Main target/scala-2.13/food-datamart.jar etl
   ```

3. Run the clustering pipeline through the API:
   - Visit http://localhost:8000/docs
   - Execute the clustering endpoint with:
     ```json
     {
       "use_sample_data": false,
       "use_datamart": true
     }
     ```
   - Or use curl:
     ```bash
     curl -X 'POST' \
       'http://localhost:8000/cluster' \
       -H 'accept: application/json' \
       -H 'Content-Type: application/json' \
       -d '{
       "use_sample_data": false,
       "use_datamart": true
     }'
     ```

### Checking Results in MS SQL Server

```bash
docker exec -it food-clustering-app /bin/bash
sqlcmd -S mssql -U sa -P 'YourStrong@Passw0rd'
USE food_clustering;
GO

-- View food products
SELECT * FROM food_products;
GO

-- View clustering results
SELECT * FROM clustering_results;
GO

-- View cluster centers
SELECT * FROM cluster_centers;
GO
```

## Architecture

The project consists of two main components:

1. **Data Mart (Scala)**: Responsible for ETL processes
   - Loading data from Parquet files
   - Preprocessing data
   - Saving data to MS SQL Server
   - Providing API for data access and result storage

2. **Clustering Service (Python)**: Responsible for machine learning
   - Loading data from MS SQL Server via data mart
   - Feature engineering and scaling
   - K-means clustering
   - Saving results to MS SQL Server via data mart
   - Providing API for clustering operations
