"""
Delta Platform Library

A Python library for managing Delta Lake data platforms with medallion architecture on Databricks Spark.

This library follows design patterns for maintainable and extensible code:
- Builder Pattern: For configuring tables
- Strategy Pattern: For pluggable transformations
- Repository Pattern: For data source abstraction
- Factory Pattern: For creating tables from various sources
- Observer Pattern: For monitoring table operations
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

# Validation framework
from delta_platform.validation import (
    ValidationRule,
    ValidationResult,
    DataValidator,
    SchemaValidation,
    UniquenessValidation,
    RangeValidation,
    NullCheckValidation,
    RegexValidation,
    CustomValidation
)

# CDC support
from delta_platform.cdc import (
    CDCStrategy,
    CDCOperation,
    CDCProcessor
)

# Factory pattern
from delta_platform.factories import TableFactory

# Observer pattern
from delta_platform.observers import (
    TableObserver,
    MetricsObserver,
    DataQualityObserver,
    LoggingObserver
)

# Exceptions
from delta_platform import exceptions

# Legacy platform-level classes (maintained for backward compatibility)
from delta_platform.platform import DeltaPlatform
from delta_platform.config import PlatformConfig
from delta_platform.layers.bronze import BronzeLayer
from delta_platform.layers.silver import SilverLayer
from delta_platform.layers.gold import GoldLayer

# Streaming
from delta_platform.streaming import StreamingTable, TriggerConfig, TriggerType

# Profiling
from delta_platform.profiling import DataProfiler, ProfileReport

# Optimization
from delta_platform.optimization import OptimizationAdvisor, Recommendation, RecommendationType

# Testing
from delta_platform.testing import MockDataSource, MockTable, TableTestCase, create_test_spark

# Unity Catalog
from delta_platform.unity_catalog import UnityCatalogTable

# Benchmarking
from delta_platform.benchmarking import PerformanceBenchmark, BenchmarkResult

# Plugins
from delta_platform.plugins import Plugin, PluginManager

__version__ = "0.3.0"

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
    # Validation
    "ValidationRule",
    "ValidationResult",
    "DataValidator",
    "SchemaValidation",
    "UniquenessValidation",
    "RangeValidation",
    "NullCheckValidation",
    "RegexValidation",
    "CustomValidation",
    # CDC
    "CDCStrategy",
    "CDCOperation",
    "CDCProcessor",
    # Factory
    "TableFactory",
    # Observers
    "TableObserver",
    "MetricsObserver",
    "DataQualityObserver",
    "LoggingObserver",
    # Streaming
    "StreamingTable",
    "TriggerConfig",
    "TriggerType",
    # Profiling
    "DataProfiler",
    "ProfileReport",
    # Optimization
    "OptimizationAdvisor",
    "Recommendation",
    "RecommendationType",
    # Testing
    "MockDataSource",
    "MockTable",
    "TableTestCase",
    "create_test_spark",
    # Unity Catalog
    "UnityCatalogTable",
    # Benchmarking
    "PerformanceBenchmark",
    "BenchmarkResult",
    # Plugins
    "Plugin",
    "PluginManager",
    # Exceptions
    "exceptions",
    # Legacy
    "DeltaPlatform",
    "PlatformConfig",
    "BronzeLayer",
    "SilverLayer",
    "GoldLayer",
]
