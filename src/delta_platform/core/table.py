"""DeltaTable class for managing individual Delta tables."""

from typing import Optional, Callable, List, Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, input_file_name
from delta.tables import DeltaTable as PyDeltaTable

from delta_platform.core.table_config import TableConfig
from delta_platform.strategies.transformation import TransformationStrategy


class DeltaTable:
    """
    Manages a single Delta table with operations like append, transform, merge, etc.

    This class follows the Single Responsibility Principle by focusing on
    operations for a single table within the medallion architecture.

    Example:
        >>> config = TableConfig(name="users", path="/data/bronze/users", layer="bronze")
        >>> table = DeltaTable(spark, config)
        >>> table.append(df)
        >>> table.transform(transformation_strategy)
        >>> table.optimize()
    """

    def __init__(self, spark: SparkSession, config: TableConfig):
        """Initialize a DeltaTable instance.

        Args:
            spark: Active SparkSession
            config: Table configuration
        """
        self.spark = spark
        self.config = config
        self._delta_table: Optional[PyDeltaTable] = None

    @property
    def exists(self) -> bool:
        """Check if the table exists.

        Returns:
            True if table exists, False otherwise
        """
        try:
            PyDeltaTable.forPath(self.spark, self.config.path)
            return True
        except Exception:
            return False

    @property
    def delta_table(self) -> PyDeltaTable:
        """Get the underlying PyDeltaTable instance.

        Returns:
            PyDeltaTable instance

        Raises:
            ValueError: If table does not exist
        """
        if not self.exists:
            raise ValueError(f"Table {self.config.name} does not exist at {self.config.path}")

        if self._delta_table is None:
            self._delta_table = PyDeltaTable.forPath(self.spark, self.config.path)

        return self._delta_table

    def read(self) -> DataFrame:
        """Read the table as a DataFrame.

        Returns:
            DataFrame containing table data

        Raises:
            ValueError: If table does not exist
        """
        if not self.exists:
            raise ValueError(f"Table {self.config.name} does not exist")

        return self.spark.read.format("delta").load(self.config.path)

    def append(
        self,
        df: DataFrame,
        add_metadata: bool = True,
        deduplicate: bool = False,
        deduplicate_columns: Optional[List[str]] = None
    ) -> "DeltaTable":
        """Append data to the table.

        Args:
            df: DataFrame to append
            add_metadata: Whether to add ingestion metadata
            deduplicate: Whether to deduplicate before appending
            deduplicate_columns: Columns to use for deduplication

        Returns:
            Self for method chaining
        """
        processed_df = df

        # Add metadata if requested
        if add_metadata:
            processed_df = self._add_metadata(processed_df)

        # Deduplicate if requested
        if deduplicate and deduplicate_columns:
            processed_df = processed_df.dropDuplicates(deduplicate_columns)

        # Write to table
        self._write(processed_df, mode="append")

        return self

    def overwrite(
        self,
        df: DataFrame,
        add_metadata: bool = True
    ) -> "DeltaTable":
        """Overwrite table data.

        Args:
            df: DataFrame to write
            add_metadata: Whether to add ingestion metadata

        Returns:
            Self for method chaining
        """
        processed_df = df

        if add_metadata:
            processed_df = self._add_metadata(processed_df)

        self._write(processed_df, mode="overwrite")

        return self

    def transform(
        self,
        strategy: TransformationStrategy,
        target_table: Optional["DeltaTable"] = None,
        **kwargs
    ) -> DataFrame:
        """Transform data using a strategy pattern.

        Args:
            strategy: Transformation strategy to apply
            target_table: Optional target table to write to
            **kwargs: Additional arguments for the strategy

        Returns:
            Transformed DataFrame
        """
        source_df = self.read()
        transformed_df = strategy.transform(source_df, **kwargs)

        if target_table:
            target_table.append(transformed_df, add_metadata=True)

        return transformed_df

    def merge(
        self,
        source_df: DataFrame,
        merge_keys: List[str],
        update_columns: Optional[List[str]] = None,
        insert_only: bool = False,
        delete_condition: Optional[str] = None
    ) -> "DeltaTable":
        """Merge (upsert) data into the table.

        Args:
            source_df: Source DataFrame with updates
            merge_keys: Columns to match on for merge
            update_columns: Columns to update (None = all except keys)
            insert_only: If True, only insert, don't update
            delete_condition: Optional condition for deleting matched rows

        Returns:
            Self for method chaining
        """
        if not self.exists:
            # If table doesn't exist, create it
            self.append(source_df, add_metadata=False)
            return self

        # Build merge condition
        merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])

        merge_builder = self.delta_table.alias("target").merge(
            source_df.alias("source"),
            merge_condition
        )

        # Handle deletes
        if delete_condition:
            merge_builder = merge_builder.whenMatchedDelete(delete_condition)

        # Handle updates
        if not insert_only and not delete_condition:
            if update_columns is None:
                update_columns = [c for c in source_df.columns if c not in merge_keys]

            update_dict = {col: f"source.{col}" for col in update_columns}
            merge_builder = merge_builder.whenMatchedUpdate(set=update_dict)

        # Handle inserts
        merge_builder = merge_builder.whenNotMatchedInsertAll()

        merge_builder.execute()

        return self

    def delete(self, condition: str) -> "DeltaTable":
        """Delete rows matching a condition.

        Args:
            condition: SQL condition for deletion

        Returns:
            Self for method chaining
        """
        self.delta_table.delete(condition)
        return self

    def update(self, condition: str, updates: Dict[str, Any]) -> "DeltaTable":
        """Update rows matching a condition.

        Args:
            condition: SQL condition for update
            updates: Dictionary of column updates

        Returns:
            Self for method chaining
        """
        self.delta_table.update(condition, updates)
        return self

    def optimize(
        self,
        where: Optional[str] = None,
        z_order_by: Optional[List[str]] = None
    ) -> "DeltaTable":
        """Optimize the table.

        Args:
            where: Optional condition to optimize specific partitions
            z_order_by: Columns to Z-order by (uses config if not specified)

        Returns:
            Self for method chaining
        """
        z_order = z_order_by or self.config.z_order_columns

        optimize_cmd = self.delta_table.optimize()

        if where:
            optimize_cmd = optimize_cmd.where(where)

        if z_order:
            optimize_cmd.executeZOrderBy(*z_order)
        else:
            optimize_cmd.executeCompaction()

        return self

    def vacuum(self, retention_hours: int = 168) -> "DeltaTable":
        """Vacuum the table to remove old files.

        Args:
            retention_hours: Retention period in hours (default 7 days)

        Returns:
            Self for method chaining
        """
        self.delta_table.vacuum(retention_hours)
        return self

    def history(self, limit: Optional[int] = None) -> DataFrame:
        """Get table history.

        Args:
            limit: Optional limit on number of history entries

        Returns:
            DataFrame with table history
        """
        history_df = self.delta_table.history()

        if limit:
            history_df = history_df.limit(limit)

        return history_df

    def version(self) -> int:
        """Get current table version.

        Returns:
            Current version number
        """
        return self.history(1).collect()[0]["version"]

    def schema(self) -> str:
        """Get table schema.

        Returns:
            Schema as string
        """
        return self.read().schema.simpleString()

    def count(self) -> int:
        """Count rows in the table.

        Returns:
            Number of rows
        """
        return self.read().count()

    def statistics(self) -> Dict[str, Any]:
        """Get table statistics.

        Returns:
            Dictionary with table statistics
        """
        return {
            "name": self.config.name,
            "path": self.config.path,
            "layer": self.config.layer.value,
            "exists": self.exists,
            "row_count": self.count() if self.exists else 0,
            "version": self.version() if self.exists else None,
            "schema": self.schema() if self.exists else None,
            "partition_columns": self.config.partition_columns,
        }

    def time_travel(
        self,
        version: Optional[int] = None,
        timestamp: Optional[str] = None
    ) -> DataFrame:
        """Read table at a specific version or timestamp (time travel).

        Args:
            version: Table version number to read
            timestamp: Timestamp string to read (format: 'YYYY-MM-DD HH:MM:SS')

        Returns:
            DataFrame at the specified version or timestamp

        Raises:
            ValueError: If neither or both version and timestamp are provided
            TableNotFoundError: If table doesn't exist

        Example:
            >>> # Read table at version 5
            >>> df_v5 = table.time_travel(version=5)
            >>>
            >>> # Read table at specific timestamp
            >>> df_yesterday = table.time_travel(timestamp="2024-01-15 10:00:00")
        """
        from delta_platform.exceptions import TableNotFoundError

        if not self.exists:
            raise TableNotFoundError(self.config.name, self.config.path)

        if version is None and timestamp is None:
            raise ValueError("Must provide either version or timestamp")

        if version is not None and timestamp is not None:
            raise ValueError("Cannot provide both version and timestamp")

        reader = self.spark.read.format("delta")

        if version is not None:
            reader = reader.option("versionAsOf", version)
        else:
            reader = reader.option("timestampAsOf", timestamp)

        return reader.load(self.config.path)

    def clone(
        self,
        target_path: str,
        shallow: bool = False,
        replace: bool = False
    ) -> "DeltaTable":
        """Clone this table to a new location.

        Args:
            target_path: Path for the cloned table
            shallow: If True, create shallow clone (metadata only)
                    If False, create deep clone (full copy)
            replace: If True, replace existing table at target

        Returns:
            New DeltaTable instance for the cloned table

        Example:
            >>> # Deep clone (full copy)
            >>> cloned = table.clone("/data/backup/users")
            >>>
            >>> # Shallow clone (metadata only, faster)
            >>> snapshot = table.clone("/data/snapshots/users", shallow=True)
        """
        from delta_platform.core.table_builder import TableBuilder

        clone_type = "SHALLOW" if shallow else "DEEP"
        replace_clause = "OR REPLACE" if replace else ""

        # Execute clone operation
        clone_sql = f"CREATE {replace_clause} TABLE delta.`{target_path}` {clone_type} CLONE delta.`{self.config.path}`"
        self.spark.sql(clone_sql)

        # Return new table instance
        return (TableBuilder(self.spark)
                .name(f"{self.config.name}_clone")
                .path(target_path)
                .layer(self.config.layer)
                .build())

    def restore(self, version: Optional[int] = None, timestamp: Optional[str] = None) -> "DeltaTable":
        """Restore table to a previous version.

        Args:
            version: Version number to restore to
            timestamp: Timestamp to restore to

        Returns:
            Self for method chaining

        Raises:
            ValueError: If neither or both version and timestamp are provided

        Example:
            >>> # Restore to version 10
            >>> table.restore(version=10)
            >>>
            >>> # Restore to yesterday
            >>> table.restore(timestamp="2024-01-15 10:00:00")
        """
        if version is None and timestamp is None:
            raise ValueError("Must provide either version or timestamp")

        if version is not None and timestamp is not None:
            raise ValueError("Cannot provide both version and timestamp")

        if version is not None:
            restore_sql = f"RESTORE TABLE delta.`{self.config.path}` TO VERSION AS OF {version}"
        else:
            restore_sql = f"RESTORE TABLE delta.`{self.config.path}` TO TIMESTAMP AS OF '{timestamp}'"

        self.spark.sql(restore_sql)

        # Clear cached delta table to force reload
        self._delta_table = None

        return self

    def generate_manifest(self, mode: str = "symlink_format_manifest") -> "DeltaTable":
        """Generate manifest files for Presto/Athena compatibility.

        Args:
            mode: Manifest mode (default: "symlink_format_manifest")

        Returns:
            Self for method chaining

        Example:
            >>> # Generate manifest for Athena/Presto
            >>> table.generate_manifest()
        """
        self.delta_table.generate(mode)
        return self

    def convert_to_delta(
        self,
        partition_schema: Optional[str] = None
    ) -> "DeltaTable":
        """Convert a Parquet table to Delta format.

        Args:
            partition_schema: Partition schema in Hive format (e.g., "date DATE, region STRING")

        Returns:
            Self for method chaining

        Example:
            >>> # Convert Parquet to Delta
            >>> table.convert_to_delta(partition_schema="year INT, month INT")
        """
        if partition_schema:
            convert_sql = f"CONVERT TO DELTA parquet.`{self.config.path}` PARTITIONED BY ({partition_schema})"
        else:
            convert_sql = f"CONVERT TO DELTA parquet.`{self.config.path}`"

        self.spark.sql(convert_sql)
        return self

    def get_details(self) -> Dict[str, Any]:
        """Get detailed table information.

        Returns:
            Dictionary with detailed table information including:
            - Format
            - Partition columns
            - Table properties
            - Statistics

        Example:
            >>> details = table.get_details()
            >>> print(details['numFiles'])
        """
        describe_df = self.spark.sql(f"DESCRIBE DETAIL delta.`{self.config.path}`")
        row = describe_df.collect()[0]

        return {
            "format": row.format,
            "id": row.id,
            "name": row.name,
            "location": row.location,
            "created_at": row.createdAt,
            "last_modified": row.lastModified,
            "partition_columns": row.partitionColumns,
            "num_files": row.numFiles,
            "size_bytes": row.sizeInBytes,
            "properties": row.properties,
            "min_reader_version": row.minReaderVersion,
            "min_writer_version": row.minWriterVersion
        }

    def _write(self, df: DataFrame, mode: str = "append") -> None:
        """Internal method to write DataFrame to table.

        Args:
            df: DataFrame to write
            mode: Write mode (append, overwrite)
        """
        writer = df.write.format("delta").mode(mode)

        # Apply partitioning
        if self.config.partition_columns:
            writer = writer.partitionBy(*self.config.partition_columns)

        # Apply schema options
        if self.config.merge_schema:
            writer = writer.option("mergeSchema", "true")

        if self.config.overwrite_schema:
            writer = writer.option("overwriteSchema", "true")

        # Apply table properties
        for key, value in self.config.table_properties.items():
            writer = writer.option(key, value)

        writer.save(self.config.path)

    def _add_metadata(self, df: DataFrame) -> DataFrame:
        """Add metadata columns to DataFrame.

        Args:
            df: Source DataFrame

        Returns:
            DataFrame with metadata columns
        """
        layer_prefix = f"_{self.config.layer.value}"

        return df \
            .withColumn(f"{layer_prefix}_ingestion_timestamp", current_timestamp()) \
            .withColumn(f"{layer_prefix}_source_file", input_file_name())

    def __repr__(self) -> str:
        """String representation."""
        return f"DeltaTable(name='{self.config.name}', layer='{self.config.layer.value}', exists={self.exists})"
