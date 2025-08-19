"""
Module for visualizing clustering results.
"""
import os
from typing import Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pyspark.sql import DataFrame

from src.utils.config import config
from src.utils.logger import logger


class ClusterVisualizer:
    """
    Class for visualizing clustering results.
    """
    def __init__(self):
        """
        Initialize cluster visualizer.
        """
        self.output_dir = "results"

        # Create output directory if it doesn't exist
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def visualize_cluster_sizes(self, cluster_sizes: Dict[int, int],
                               filename: str = "cluster_sizes.png") -> str:
        """
        Visualize cluster sizes.

        Args:
            cluster_sizes: Dictionary with cluster indices and their sizes.
            filename: Output filename.

        Returns:
            Path to the saved visualization.
        """
        logger.info("Visualizing cluster sizes...")

        # Create figure
        plt.figure(figsize=(10, 6))

        # Sort clusters by index
        clusters = sorted(cluster_sizes.keys())
        sizes = [cluster_sizes[cluster] for cluster in clusters]

        # Create bar chart
        plt.bar(clusters, sizes, color='skyblue')

        # Add labels and title
        plt.xlabel('Cluster')
        plt.ylabel('Number of Products')
        plt.title('Number of Products in Each Cluster')

        # Add value labels on top of bars
        for i, v in enumerate(sizes):
            plt.text(i, v + 0.1, str(v), ha='center')

        # Set x-ticks to cluster indices
        plt.xticks(clusters)

        # Save figure
        output_path = os.path.join(self.output_dir, filename)
        plt.savefig(output_path)
        plt.close()

        logger.info(f"Cluster sizes visualization saved to {output_path}")
        return output_path

    def visualize_cluster_centers(self, cluster_centers: List[List[float]],
                                 feature_names: List[str],
                                 filename: str = "cluster_centers.png") -> str:
        """
        Visualize cluster centers.

        Args:
            cluster_centers: List of cluster centers.
            feature_names: List of feature names.
            filename: Output filename.

        Returns:
            Path to the saved visualization.
        """
        logger.info("Visualizing cluster centers...")

        # Create figure
        plt.figure(figsize=(12, 8))

        # Number of clusters and features
        n_clusters = len(cluster_centers)
        n_features = len(feature_names)

        # Create a DataFrame for easier plotting
        centers_df = pd.DataFrame(cluster_centers, columns=feature_names)

        # Add cluster column
        centers_df['Cluster'] = [f'Cluster {i}' for i in range(n_clusters)]

        # Set cluster as index
        centers_df = centers_df.set_index('Cluster')

        # Create heatmap
        plt.imshow(centers_df.values, cmap='viridis', aspect='auto')

        # Add colorbar
        plt.colorbar(label='Value')

        # Add labels and title
        plt.xlabel('Feature')
        plt.ylabel('Cluster')
        plt.title('Cluster Centers')

        # Set x-ticks to feature names
        plt.xticks(range(n_features), feature_names, rotation=45, ha='right')

        # Set y-ticks to cluster indices
        plt.yticks(range(n_clusters), centers_df.index)

        # Add values to cells
        for i in range(n_clusters):
            for j in range(n_features):
                plt.text(j, i, f'{centers_df.iloc[i, j]:.2f}',
                        ha='center', va='center', color='white')

        # Adjust layout
        plt.tight_layout()

        # Save figure
        output_path = os.path.join(self.output_dir, filename)
        plt.savefig(output_path)
        plt.close()

        logger.info(f"Cluster centers visualization saved to {output_path}")
        return output_path

    def visualize_feature_distribution(self, df: DataFrame,
                                      feature_name: str,
                                      filename: Optional[str] = None) -> str:
        """
        Visualize feature distribution by cluster.

        Args:
            df: DataFrame with cluster predictions.
            feature_name: Feature name to visualize.
            filename: Output filename. If None, feature name will be used.

        Returns:
            Path to the saved visualization.
        """
        logger.info(f"Visualizing distribution of {feature_name} by cluster...")

        # Set filename if not provided
        if filename is None:
            filename = f"{feature_name}_distribution.png"

        # Convert to Pandas DataFrame for easier plotting
        pdf = df.select("cluster", feature_name).toPandas()

        # Create figure
        plt.figure(figsize=(12, 6))

        # Create boxplot
        plt.boxplot([pdf[pdf['cluster'] == i][feature_name] for i in range(df.select('cluster').distinct().count())],
                   labels=[f'Cluster {i}' for i in range(df.select('cluster').distinct().count())])

        # Add labels and title
        plt.xlabel('Cluster')
        plt.ylabel(feature_name)
        plt.title(f'Distribution of {feature_name} by Cluster')

        # Save figure
        output_path = os.path.join(self.output_dir, filename)
        plt.savefig(output_path)
        plt.close()

        logger.info(f"Feature distribution visualization saved to {output_path}")
        return output_path

    def visualize_2d_clusters(self, df: DataFrame,
                             feature1: str,
                             feature2: str,
                             filename: Optional[str] = None) -> str:
        """
        Visualize clusters in 2D space.

        Args:
            df: DataFrame with cluster predictions.
            feature1: First feature for x-axis.
            feature2: Second feature for y-axis.
            filename: Output filename. If None, feature names will be used.

        Returns:
            Path to the saved visualization.
        """
        logger.info(f"Visualizing clusters in 2D space ({feature1} vs {feature2})...")

        # Set filename if not provided
        if filename is None:
            filename = f"{feature1}_vs_{feature2}.png"

        # Convert to Pandas DataFrame for easier plotting
        pdf = df.select("cluster", feature1, feature2).toPandas()

        # Create figure
        plt.figure(figsize=(10, 8))

        # Get unique clusters
        clusters = pdf['cluster'].unique()

        # Create scatter plot for each cluster
        for cluster in clusters:
            cluster_data = pdf[pdf['cluster'] == cluster]
            plt.scatter(cluster_data[feature1], cluster_data[feature2],
                       label=f'Cluster {cluster}', alpha=0.7)

        # Add labels and title
        plt.xlabel(feature1)
        plt.ylabel(feature2)
        plt.title(f'Clusters in 2D Space ({feature1} vs {feature2})')

        # Add legend
        plt.legend()

        # Add grid
        plt.grid(True, linestyle='--', alpha=0.7)

        # Save figure
        output_path = os.path.join(self.output_dir, filename)
        plt.savefig(output_path)
        plt.close()

        logger.info(f"2D clusters visualization saved to {output_path}")
        return output_path

    def generate_cluster_report(self, cluster_centers: List[List[float]],
                               feature_names: List[str],
                               cluster_sizes: Dict[int, int],
                               cluster_samples: Dict[int, List[Dict]],
                               filename: str = "cluster_report.html") -> str:
        """
        Generate HTML report for clusters.

        Args:
            cluster_centers: List of cluster centers.
            feature_names: List of feature names.
            cluster_sizes: Dictionary with cluster indices and their sizes.
            cluster_samples: Dictionary with cluster indices and lists of sample products.
            filename: Output filename.

        Returns:
            Path to the saved report.
        """
        logger.info("Generating cluster report...")

        # Create HTML content
        html_content = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Clustering Report</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; }
                h1 { color: #2c3e50; }
                h2 { color: #3498db; }
                table { border-collapse: collapse; width: 100%; margin-bottom: 20px; }
                th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
                th { background-color: #f2f2f2; }
                tr:nth-child(even) { background-color: #f9f9f9; }
                .cluster-section { margin-bottom: 30px; border: 1px solid #ddd; padding: 15px; border-radius: 5px; }
                .highlight { font-weight: bold; color: #e74c3c; }
            </style>
        </head>
        <body>
            <h1>Food Products Clustering Report</h1>
        """

        # Add cluster summary
        html_content += """
            <h2>Cluster Summary</h2>
            <table>
                <tr>
                    <th>Cluster</th>
                    <th>Size</th>
                    <th>Percentage</th>
                </tr>
        """

        # Calculate total number of products
        total_products = sum(cluster_sizes.values())

        # Add row for each cluster
        for cluster in sorted(cluster_sizes.keys()):
            size = cluster_sizes[cluster]
            percentage = (size / total_products) * 100
            html_content += f"""
                <tr>
                    <td>Cluster {cluster}</td>
                    <td>{size}</td>
                    <td>{percentage:.2f}%</td>
                </tr>
            """

        html_content += """
            </table>
        """

        # Add cluster centers
        html_content += """
            <h2>Cluster Centers</h2>
            <table>
                <tr>
                    <th>Cluster</th>
        """

        # Add column for each feature
        for feature in feature_names:
            html_content += f"<th>{feature}</th>"

        html_content += """
                </tr>
        """

        # Add row for each cluster
        for i, center in enumerate(cluster_centers):
            html_content += f"""
                <tr>
                    <td>Cluster {i}</td>
            """

            # Add value for each feature
            for value in center:
                html_content += f"<td>{value:.4f}</td>"

            html_content += """
                </tr>
            """

        html_content += """
            </table>
        """

        # Add cluster details
        html_content += """
            <h2>Cluster Details</h2>
        """

        # Add section for each cluster
        for cluster in sorted(cluster_samples.keys()):
            samples = cluster_samples[cluster]
            center = cluster_centers[cluster]

            html_content += f"""
            <div class="cluster-section">
                <h3>Cluster {cluster}</h3>
                <p>Size: {cluster_sizes[cluster]} products ({(cluster_sizes[cluster] / total_products) * 100:.2f}% of total)</p>

                <h4>Cluster Center:</h4>
                <table>
                    <tr>
            """

            # Add column for each feature
            for feature in feature_names:
                html_content += f"<th>{feature}</th>"

            html_content += """
                    </tr>
                    <tr>
            """

            # Add value for each feature
            for value in center:
                html_content += f"<td>{value:.4f}</td>"

            html_content += """
                    </tr>
                </table>

                <h4>Sample Products:</h4>
                <table>
                    <tr>
                        <th>Code</th>
                        <th>Product Name</th>
            """

            # Add column for each feature
            for feature in feature_names:
                html_content += f"<th>{feature}</th>"

            html_content += """
                    </tr>
            """

            # Add row for each sample
            for sample in samples:
                html_content += f"""
                    <tr>
                        <td>{sample.get('code', 'N/A')}</td>
                        <td>{sample.get('product_name', 'N/A')}</td>
                """

                # Add value for each feature
                for feature in feature_names:
                    html_content += f"<td>{sample.get(feature, 'N/A')}</td>"

                html_content += """
                    </tr>
                """

            html_content += """
                </table>
            </div>
            """

        # Close HTML
        html_content += """
        </body>
        </html>
        """

        # Save HTML report
        output_path = os.path.join(self.output_dir, filename)
        with open(output_path, 'w') as f:
            f.write(html_content)

        logger.info(f"Cluster report saved to {output_path}")
        return output_path
