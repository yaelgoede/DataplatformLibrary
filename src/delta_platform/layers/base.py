"""Base layer class for medallion architecture."""

from abc import ABC, abstractmethod
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable


class BaseLayer(ABC):
    """Base class for all medallion architecture layers."""

    def __init__(
        self,
        spark: SparkSession,
        layer_path: str,
        layer_name: str,
        checkpoint_path: Optional[str] = None
    ):
        """Initialize the base layer.

        Args:
            spark: Active SparkSession
            layer_path: Base path for the layer's data
            layer_name: Name of the layer (bronze, silver, gold)
            checkpoint_path: Path for streaming checkpoints
        """
        self.spark = spark
        self.layer_path = layer_path
        self.layer_name = layer_name
        self.checkpoint_path = checkpoint_path or f"{layer_path}/_checkpoints"

    def table_path(self, table_name: str) -> str:
        """Get the full path for a table in this layer.

        Args:
            table_name: Name of the table

        Returns:
            Full path to the table
        """
        return f"{self.layer_path}/{table_name}"

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in this layer.

        Args:
            table_name: Name of the table

        Returns:
            True if table exists, False otherwise
        """
        try:
            DeltaTable.forPath(self.spark, self.table_path(table_name))
            return True
        except Exception:
            return False

    def read_table(self, table_name: str) -> DataFrame:
        """Read a Delta table from this layer.

        Args:
            table_name: Name of the table

        Returns:
            DataFrame containing the table data
        """
        return self.spark.read.format("delta").load(self.table_path(table_name))

    def write_table(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "append",
        partition_by: Optional[list[str]] = None,
        merge_schema: bool = True
    ) -> None:
        """Write a DataFrame to a Delta table.

        Args:
            df: DataFrame to write
            table_name: Name of the table
            mode: Write mode (append, overwrite, etc.)
            partition_by: Columns to partition by
            merge_schema: Whether to merge schemas
        """
        writer = df.write.format("delta").mode(mode)

        if partition_by:
            writer = writer.partitionBy(*partition_by)

        if merge_schema:
            writer = writer.option("mergeSchema", "true")

        writer.save(self.table_path(table_name))

    def get_delta_table(self, table_name: str) -> DeltaTable:
        """Get a DeltaTable instance.

        Args:
            table_name: Name of the table

        Returns:
            DeltaTable instance
        """
        return DeltaTable.forPath(self.spark, self.table_path(table_name))

    def optimize_table(self, table_name: str, z_order_by: Optional[list[str]] = None) -> None:
        """Optimize a Delta table.

        Args:
            table_name: Name of the table
            z_order_by: Columns to Z-order by
        """
        delta_table = self.get_delta_table(table_name)
        optimize_cmd = delta_table.optimize()

        if z_order_by:
            optimize_cmd.executeZOrderBy(*z_order_by)
        else:
            optimize_cmd.executeCompaction()

    def vacuum_table(self, table_name: str, retention_hours: int = 168) -> None:
        """Vacuum a Delta table to remove old files.

        Args:
            table_name: Name of the table
            retention_hours: Retention period in hours (default 7 days)
        """
        delta_table = self.get_delta_table(table_name)
        delta_table.vacuum(retention_hours)

    @abstractmethod
    def process(self, *args, **kwargs) -> DataFrame:
        """Process data for this layer. Must be implemented by subclasses."""
        pass
