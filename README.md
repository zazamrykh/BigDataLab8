# Food Products Clustering

This project implements a K-means clustering model for food products using PySpark. It analyzes nutritional information from the OpenFoodFacts dataset to identify patterns and group similar products together. The project now includes integration with MS SQL Server for data storage and a FastAPI service for API access.

## Project Structure

```
.
├── config.ini                 # Configuration file
├── docker-compose.yml         # Docker Compose configuration
├── .env                       # Environment variables for Docker
├── src/                       # Source code
│   ├── api/                   # API service
│   │   ├── __init__.py        # API package initialization
│   │   ├── main.py            # FastAPI application
│   │   └── models.py          # Pydantic models for API
│   ├── data/                  # Data loading and preprocessing
│   │   ├── loader.py          # Data loading module (file and MS SQL)
│   │   ├── preprocessor.py    # Data preprocessing module
│   │   └── saver.py           # Data saving module (MS SQL)
│   ├── features/              # Feature engineering
│   │   └── scaler.py          # Feature scaling module
│   ├── models/                # Machine learning models
│   │   ├── kmeans.py          # K-means clustering model
│   │   └── model_loader.py    # Model loading module
│   ├── utils/                 # Utilities
│   │   ├── config.py          # Configuration utilities
│   │   └── logger.py          # Logging utilities
│   ├── visualization/         # Visualization utilities
│   │   └── visualizer.py      # Visualization module
│   └── main.py                # Main application entry point
├── docker/                    # Docker configuration
│   └── Dockerfile             # Dockerfile for the application
├── scripts/                   # Scripts for initialization
│   └── mssql/                 # MS SQL Server scripts
│       ├── 01_create_database.sql  # Script to create database
│       ├── 02_create_tables.sql    # Script to create tables
│       └── 03_insert_test_data.sql # Script to insert test data
├── tests/                     # Unit tests
├── results/                   # Output directory for visualizations and reports
└── model/                     # Directory for saved models
```

## Requirements

- Python 3.12+
- PySpark 4.0.0+
- Java 8+
- pandas
- matplotlib
- numpy
- FastAPI
- uvicorn
- pyodbc/pymssql (for MS SQL Server connection)
- Docker and Docker Compose (for containerized deployment)

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
- `SAVED_MODEL`: Path to saved model

Environment variables for Docker deployment are stored in the `.env` file.

## Usage

### Running the Application Locally

To run the application with default settings (file-based data):

```bash
python src/main.py
```

To run with sample data (for testing):

```bash
python src/main.py --sample
```

To save the trained model:

```bash
python src/main.py --save-model
```

### Running the FastAPI Service

To run the FastAPI service locally:

```bash
python src/api/main.py
```

The API will be available at http://localhost:8000 with the following endpoints:
- GET /health - Health check endpoint
- POST /cluster - Clustering endpoint

### Running with Docker

To run the application with Docker:

```bash
# Build and start the containers
docker-compose up -d

# Check the logs
docker-compose logs -f app

# Stop the containers
docker-compose down
```

This will start:
1. MS SQL Server container
2. Application container with FastAPI service

The API will be available at http://localhost:8000

## Implementation Details

### Data Processing Pipeline

1. **Data Loading**: The application can load data from:
   - OpenFoodFacts dataset (CSV, JSON, or Parquet file)
   - MS SQL Server database

2. **Preprocessing**: The data is preprocessed by:
   - Extracting nutriment features
   - Removing rows with null values
   - Removing outliers using Z-score

3. **Feature Engineering**: The features are scaled using one of the following methods:
   - Standard scaling (z-score normalization)
   - Min-max scaling
   - Max-abs scaling

4. **Clustering**: The data is clustered using K-means algorithm with the configured number of clusters.

5. **Evaluation**: The clustering is evaluated using the Silhouette score.

6. **Results Storage**: The clustering results can be:
   - Visualized and saved as images
   - Saved to MS SQL Server database
   - Accessed via API

7. **API Access**: The results can be accessed via FastAPI service:
   - Health check endpoint
   - Clustering endpoint

### MS SQL Server Integration

The application integrates with MS SQL Server for:

1. **Data Source**: Loading data from MS SQL Server tables
2. **Results Storage**: Saving clustering results to MS SQL Server tables:
   - `food_products`: Table for storing food products data
   - `clustering_results`: Table for storing clustering results
   - `cluster_centers`: Table for storing cluster centers

### Docker Deployment

The application can be deployed using Docker:

1. **MS SQL Server Container**: Official Microsoft SQL Server image
2. **Application Container**: Custom image with PySpark and FastAPI
3. **Docker Compose**: Orchestration of containers

## Results

The clustering results are saved in:

1. **Results Directory**: Visualizations and reports
   - Cluster sizes and centers
   - Feature distributions by cluster
   - 2D scatter plots of clusters
   - HTML report with detailed cluster information

2. **MS SQL Server Database**:
   - Clustering results with product IDs and cluster assignments
   - Cluster centers with feature values

3. **API Responses**: JSON responses with clustering information

## Future Work

- Integration with other database systems
- Deployment to a Kubernetes cluster
- Implementation of additional clustering algorithms
- Interactive visualization dashboard
- Real-time clustering with streaming data
- Authentication and authorization for API access
