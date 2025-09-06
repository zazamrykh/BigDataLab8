"""
Pydantic models for API.
"""
from typing import Dict, List, Optional
from pydantic import BaseModel, Field


class ClusteringRequest(BaseModel):
    """
    Request model for clustering.
    """
    use_sample_data: bool = Field(False, description="Whether to use sample data")
    use_datamart: bool = Field(False, description="Whether to use Data Mart API instead of direct MSSQL access")


class ClusteringResponse(BaseModel):
    """
    Response model for clustering.
    """
    success: bool = Field(..., description="Whether the clustering was successful")
    message: str = Field(..., description="Message about the clustering")
    cluster_count: int = Field(..., description="Number of clusters")
    silhouette_score: float = Field(..., description="Silhouette score")
    cluster_sizes: Dict[str, int] = Field(..., description="Sizes of clusters")


class HealthResponse(BaseModel):
    """
    Response model for health check.
    """
    status: str = Field(..., description="Status of the service")
    version: str = Field(..., description="Version of the service")
