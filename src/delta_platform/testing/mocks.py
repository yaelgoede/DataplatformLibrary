"""Mock objects for testing."""

from typing import Optional, Dict, Any
from pyspark.sql import DataFrame, SparkSession

from delta_platform.repositories.data_source import DataSourceRepository
from delta_platform.core.table import DeltaTable
from delta_platform.core.table_config import TableConfig, MedallionLayer


class MockDataSource(DataSourceRepository):
    """Mock data source for testing."""

    def __init__(self, spark: SparkSession, df: DataFrame):
        """Initialize mock data source.

        Args:
            spark: SparkSession
            df: DataFrame to return
        """
        super().__init__(spark, "/mock/path")
        self.df = df

    def read(self, **options) -> DataFrame:
        """Return mock DataFrame."""
        return self.df

    def read_stream(self, **options) -> DataFrame:
        """Return mock streaming DataFrame."""
        return self.df


class MockTable(DeltaTable):
    """Mock Delta table for testing."""

    def __init__(self, spark: SparkSession, name: str = "mock_table"):
        """Initialize mock table.

        Args:
            spark: SparkSession
            name: Table name
        """
        config = TableConfig(
            name=name,
            path=f"/tmp/test/{name}",
            layer=MedallionLayer.BRONZE
        )
        super().__init__(spark, config)
        self._mock_exists = False
        self._mock_data: Optional[DataFrame] = None

    def set_data(self, df: DataFrame):
        """Set mock data."""
        self._mock_data = df
        self._mock_exists = True

    @property
    def exists(self) -> bool:
        """Override exists check."""
        return self._mock_exists

    def read(self) -> DataFrame:
        """Return mock data."""
        if self._mock_data is None:
            raise ValueError("No mock data set")
        return self._mock_data
