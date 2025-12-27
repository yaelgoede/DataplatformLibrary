"""
Delta Platform Library

A Python library for managing Delta Lake data platforms with medallion architecture on Databricks Spark.

This library follows design patterns for maintainable and extensible code:
- Builder Pattern: For configuring tables
- Strategy Pattern: For pluggable transformations
- Repository Pattern: For data source abstraction
"""

# Core components (recommended for new code)
from delta_platform.core.table import DeltaTable
from delta_platform.core.table_builder import TableBuilder
from delta_platform.core.table_config import TableConfig, MedallionLayer

# Transformation strategies
from delta_platform.strategies.transformation import (
    TransformationStrategy,
    CleaningStrategy,
    DeduplicationStrategy,
    FilterStrategy,
    AggregationStrategy,
    CustomStrategy
)

# Data source repositories
from delta_platform.repositories.data_source import (
    DataSourceRepository,
    JsonDataSource,
    XmlDataSource,
    ParquetDataSource,
    CsvDataSource,
    DeltaDataSource
)

# Legacy platform-level classes (maintained for backward compatibility)
from delta_platform.platform import DeltaPlatform
from delta_platform.config import PlatformConfig
from delta_platform.layers.bronze import BronzeLayer
from delta_platform.layers.silver import SilverLayer
from delta_platform.layers.gold import GoldLayer

__version__ = "0.2.0"

__all__ = [
    # Core (recommended)
    "DeltaTable",
    "TableBuilder",
    "TableConfig",
    "MedallionLayer",
    # Strategies
    "TransformationStrategy",
    "CleaningStrategy",
    "DeduplicationStrategy",
    "FilterStrategy",
    "AggregationStrategy",
    "CustomStrategy",
    # Repositories
    "DataSourceRepository",
    "JsonDataSource",
    "XmlDataSource",
    "ParquetDataSource",
    "CsvDataSource",
    "DeltaDataSource",
    # Legacy
    "DeltaPlatform",
    "PlatformConfig",
    "BronzeLayer",
    "SilverLayer",
    "GoldLayer",
]
