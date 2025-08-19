# Food Products Clustering

This project implements a K-means clustering model for food products using PySpark. It analyzes nutritional information from the OpenFoodFacts dataset to identify patterns and group similar products together.

## Project Structure

```
.
├── config.ini                 # Configuration file
├── src/                       # Source code
│   ├── data/                  # Data loading and preprocessing
│   │   ├── loader.py          # Data loading module
│   │   └── preprocessor.py    # Data preprocessing module
│   ├── features/              # Feature engineering
│   │   └── scaler.py          # Feature scaling module
│   ├── models/                # Machine learning models
│   │   └── kmeans.py          # K-means clustering model
│   ├── utils/                 # Utilities
│   │   ├── config.py          # Configuration utilities
│   │   └── logger.py          # Logging utilities
│   ├── visualization/         # Visualization utilities
│   │   └── visualizer.py      # Visualization module
│   └── main.py                # Main application entry point
├── tests/                     # Unit tests
│   ├── test_config.py         # Tests for config module
│   ├── test_data_loader.py    # Tests for data loader module
│   ├── test_kmeans.py         # Tests for K-means module
│   ├── test_logger.py         # Tests for logger module
│   ├── test_main.py           # Tests for main module
│   ├── test_preprocessor.py   # Tests for preprocessor module
│   ├── test_scaler.py         # Tests for scaler module
│   └── test_visualizer.py     # Tests for visualizer module
├── results/                   # Output directory for visualizations and reports
└── model/                     # Directory for saved models
```

## Requirements

- Python 3.8+
- PySpark 3.1+
- Java 8+
- pandas
- matplotlib
- numpy

## Configuration

The application is configured using the `config.ini` file. The main configuration sections are:

- `DEFAULT`: General settings like logging level and random seed
- `DATA`: Data loading settings like URL and sample size
- `PREPROCESSING`: Data preprocessing settings like outlier threshold
- `FEATURES`: Feature engineering settings like feature list and scaling method
- `MODEL`: Model settings like number of clusters and iterations
- `SPARK`: Spark configuration settings like memory allocation and parallelism

## Usage

### Running the Application

To run the application with default settings:

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

### Running Tests

To run all tests:

```bash
python -m unittest discover tests
```

To run a specific test:

```bash
python -m unittest tests.test_config
```

## Implementation Details

### Data Processing Pipeline

1. **Data Loading**: The application loads data from the OpenFoodFacts dataset, either from a CSV file or JSON file.

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

6. **Visualization**: The results are visualized using:
   - Cluster size distribution
   - Cluster centers heatmap
   - Feature distributions by cluster
   - 2D scatter plots of clusters
   - HTML report with detailed cluster information

### Spark Configuration

The application is configured to use Spark efficiently by:

- Setting appropriate memory allocation for driver and executors
- Configuring the number of executors and cores
- Setting the number of shuffle partitions and default parallelism
- Using KryoSerializer for better performance

## Results

The clustering results are saved in the `results` directory, including:

- Visualizations of cluster sizes and centers
- Feature distributions by cluster
- 2D scatter plots of clusters
- HTML report with detailed cluster information

The trained model can be saved in the `model` directory for future use.

## Future Work

- Integration with a database for storing results
- Deployment to a Kubernetes cluster
- Implementation of additional clustering algorithms
- Interactive visualization dashboard
