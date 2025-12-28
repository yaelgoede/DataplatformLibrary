"""Test fixtures and utilities."""

import tempfile
import shutil
from typing import Optional
from pyspark.sql import SparkSession


def create_test_spark(app_name: str = "DeltaPlatformTest") -> SparkSession:
    """Create a SparkSession for testing.

    Args:
        app_name: Application name

    Returns:
        SparkSession configured for testing

    Example:
        >>> spark = create_test_spark()
        >>> # Run tests
        >>> spark.stop()
    """
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.default.parallelism", "2") \
        .getOrCreate()


class TableTestCase:
    """Base class for testing table operations."""

    def __init__(self):
        """Initialize test case."""
        self.spark: Optional[SparkSession] = None
        self.temp_dir: Optional[str] = None

    def setup(self):
        """Set up test environment."""
        self.spark = create_test_spark()
        self.temp_dir = tempfile.mkdtemp()

    def teardown(self):
        """Clean up test environment."""
        if self.spark:
            self.spark.stop()
        if self.temp_dir:
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    def create_test_table(self, name: str, layer_path: Optional[str] = None):
        """Create a table for testing.

        Args:
            name: Table name
            layer_path: Custom layer path

        Returns:
            DeltaTable instance for testing
        """
        from delta_platform.core.table_builder import TableBuilder
        from delta_platform.core.table_config import MedallionLayer

        path = layer_path or f"{self.temp_dir}/{name}"

        return (TableBuilder(self.spark)
                .name(name)
                .path(path)
                .layer(MedallionLayer.BRONZE)
                .build())

    def __enter__(self):
        """Context manager entry."""
        self.setup()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.teardown()
