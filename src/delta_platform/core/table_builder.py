"""Builder pattern for creating DeltaTable instances."""

from typing import Optional, List, Dict, Any
from pyspark.sql import SparkSession

from delta_platform.core.table import DeltaTable
from delta_platform.core.table_config import TableConfig, MedallionLayer


class TableBuilder:
    """
    Builder pattern for configuring and creating DeltaTable instances.

    This provides a fluent interface for building complex table configurations.

    Example:
        >>> table = (TableBuilder(spark)
        ...     .name("users")
        ...     .path("/data/bronze/users")
        ...     .layer(MedallionLayer.BRONZE)
        ...     .partition_by(["date", "region"])
        ...     .optimize_write(True)
        ...     .z_order_by(["user_id"])
        ...     .build())
    """

    def __init__(self, spark: SparkSession):
        """Initialize the builder.

        Args:
            spark: Active SparkSession
        """
        self._spark = spark
        self._name: Optional[str] = None
        self._path: Optional[str] = None
        self._layer: Optional[MedallionLayer] = None
        self._partition_columns: List[str] = []
        self._merge_schema: bool = True
        self._overwrite_schema: bool = False
        self._optimize_write: bool = True
        self._auto_compact: bool = True
        self._z_order_columns: List[str] = []
        self._checkpoint_path: Optional[str] = None
        self._table_properties: Dict[str, str] = {}
        self._description: Optional[str] = None
        self._tags: Dict[str, str] = {}

    def name(self, name: str) -> "TableBuilder":
        """Set the table name.

        Args:
            name: Table name

        Returns:
            Self for method chaining
        """
        self._name = name
        return self

    def path(self, path: str) -> "TableBuilder":
        """Set the table path.

        Args:
            path: Full path to table location

        Returns:
            Self for method chaining
        """
        self._path = path
        return self

    def layer(self, layer: MedallionLayer) -> "TableBuilder":
        """Set the medallion layer.

        Args:
            layer: Medallion layer (BRONZE, SILVER, GOLD)

        Returns:
            Self for method chaining
        """
        self._layer = layer
        return self

    def partition_by(self, columns: List[str]) -> "TableBuilder":
        """Set partition columns.

        Args:
            columns: List of column names to partition by

        Returns:
            Self for method chaining
        """
        self._partition_columns = columns
        return self

    def merge_schema(self, enabled: bool = True) -> "TableBuilder":
        """Enable or disable schema merging.

        Args:
            enabled: Whether to enable schema merging

        Returns:
            Self for method chaining
        """
        self._merge_schema = enabled
        return self

    def overwrite_schema(self, enabled: bool = True) -> "TableBuilder":
        """Enable or disable schema overwriting.

        Args:
            enabled: Whether to enable schema overwriting

        Returns:
            Self for method chaining
        """
        self._overwrite_schema = enabled
        return self

    def optimize_write(self, enabled: bool = True) -> "TableBuilder":
        """Enable or disable optimized writes.

        Args:
            enabled: Whether to enable optimized writes

        Returns:
            Self for method chaining
        """
        self._optimize_write = enabled
        return self

    def auto_compact(self, enabled: bool = True) -> "TableBuilder":
        """Enable or disable auto compaction.

        Args:
            enabled: Whether to enable auto compaction

        Returns:
            Self for method chaining
        """
        self._auto_compact = enabled
        return self

    def z_order_by(self, columns: List[str]) -> "TableBuilder":
        """Set Z-order columns for optimization.

        Args:
            columns: Columns to Z-order by

        Returns:
            Self for method chaining
        """
        self._z_order_columns = columns
        return self

    def checkpoint_path(self, path: str) -> "TableBuilder":
        """Set checkpoint path for streaming.

        Args:
            path: Checkpoint path

        Returns:
            Self for method chaining
        """
        self._checkpoint_path = path
        return self

    def table_property(self, key: str, value: str) -> "TableBuilder":
        """Add a table property.

        Args:
            key: Property key
            value: Property value

        Returns:
            Self for method chaining
        """
        self._table_properties[key] = value
        return self

    def description(self, description: str) -> "TableBuilder":
        """Set table description.

        Args:
            description: Table description

        Returns:
            Self for method chaining
        """
        self._description = description
        return self

    def tag(self, key: str, value: str) -> "TableBuilder":
        """Add a tag to the table.

        Args:
            key: Tag key
            value: Tag value

        Returns:
            Self for method chaining
        """
        self._tags[key] = value
        return self

    def build(self) -> DeltaTable:
        """Build the DeltaTable instance.

        Returns:
            Configured DeltaTable instance

        Raises:
            ValueError: If required fields are missing
        """
        if not self._name:
            raise ValueError("Table name is required")
        if not self._path:
            raise ValueError("Table path is required")
        if not self._layer:
            raise ValueError("Table layer is required")

        config = TableConfig(
            name=self._name,
            path=self._path,
            layer=self._layer,
            partition_columns=self._partition_columns,
            merge_schema=self._merge_schema,
            overwrite_schema=self._overwrite_schema,
            optimize_write=self._optimize_write,
            auto_compact=self._auto_compact,
            z_order_columns=self._z_order_columns,
            checkpoint_path=self._checkpoint_path,
            table_properties=self._table_properties,
            description=self._description,
            tags=self._tags
        )

        return DeltaTable(self._spark, config)

    @classmethod
    def from_config(cls, spark: SparkSession, config: TableConfig) -> DeltaTable:
        """Create a DeltaTable from an existing configuration.

        Args:
            spark: Active SparkSession
            config: Table configuration

        Returns:
            DeltaTable instance
        """
        return DeltaTable(spark, config)
