"""CDC processor for applying changes to Delta tables."""

from typing import List, Optional
from pyspark.sql import DataFrame

from delta_platform.core.table import DeltaTable
from delta_platform.cdc.strategies import CDCStrategy, CDCOperation


class CDCProcessor:
    """Processor for applying CDC changes to Delta tables."""

    def __init__(
        self,
        cdc_strategy: CDCStrategy,
        delete_operation: str = CDCOperation.DELETE.value
    ):
        """Initialize CDC processor.

        Args:
            cdc_strategy: CDC processing strategy
            delete_operation: Operation value indicating deletion
        """
        self.cdc_strategy = cdc_strategy
        self.delete_operation = delete_operation

    def apply_changes(
        self,
        target_table: DeltaTable,
        cdc_df: DataFrame,
        primary_keys: List[str],
        update_columns: Optional[List[str]] = None
    ) -> DeltaTable:
        """Apply CDC changes to target table.

        Args:
            target_table: Target Delta table
            cdc_df: CDC DataFrame
            primary_keys: Primary key columns
            update_columns: Columns to update (None = all except keys)

        Returns:
            Updated table (for method chaining)

        Example:
            >>> cdc_processor = CDCProcessor(CDCStrategy())
            >>> cdc_processor.apply_changes(
            ...     target_table=users_table,
            ...     cdc_df=cdc_events,
            ...     primary_keys=["user_id"]
            ... )
        """
        # Process CDC events
        processed_cdc = self.cdc_strategy.transform(cdc_df, primary_keys=primary_keys)

        if not target_table.exists:
            # If table doesn't exist, do initial load (filter out deletes)
            initial_load = processed_cdc.filter(
                processed_cdc[self.cdc_strategy.operation_column] != self.delete_operation
            ).drop(self.cdc_strategy.operation_column)

            target_table.append(initial_load, add_metadata=False)
            return target_table

        # Separate inserts/updates from deletes
        upserts = processed_cdc.filter(
            processed_cdc[self.cdc_strategy.operation_column] != self.delete_operation
        )
        deletes = processed_cdc.filter(
            processed_cdc[self.cdc_strategy.operation_column] == self.delete_operation
        )

        # Apply deletes
        if deletes.count() > 0:
            # Build delete condition
            delete_conditions = []
            for key in primary_keys:
                delete_values = [row[key] for row in deletes.select(key).distinct().collect()]
                if delete_values:
                    values_str = ", ".join([f"'{v}'" if isinstance(v, str) else str(v) for v in delete_values])
                    delete_conditions.append(f"{key} IN ({values_str})")

            if delete_conditions:
                delete_condition = " AND ".join(delete_conditions)
                target_table.delete(delete_condition)

        # Apply upserts
        if upserts.count() > 0:
            # Drop operation column before merge
            upserts = upserts.drop(self.cdc_strategy.operation_column)

            target_table.merge(
                source_df=upserts,
                merge_keys=primary_keys,
                update_columns=update_columns
            )

        return target_table

    def read_change_feed(
        self,
        table: DeltaTable,
        starting_version: Optional[int] = None,
        starting_timestamp: Optional[str] = None,
        ending_version: Optional[int] = None,
        ending_timestamp: Optional[str] = None
    ) -> DataFrame:
        """Read change data feed from a Delta table.

        Args:
            table: Delta table to read changes from
            starting_version: Starting version (inclusive)
            starting_timestamp: Starting timestamp
            ending_version: Ending version (inclusive)
            ending_timestamp: Ending timestamp

        Returns:
            DataFrame with change data feed

        Example:
            >>> # Read all changes since version 10
            >>> changes = processor.read_change_feed(table, starting_version=10)
            >>>
            >>> # Read changes between timestamps
            >>> changes = processor.read_change_feed(
            ...     table,
            ...     starting_timestamp="2024-01-01",
            ...     ending_timestamp="2024-01-31"
            ... )
        """
        reader = table.spark.read.format("delta") \
            .option("readChangeFeed", "true")

        if starting_version is not None:
            reader = reader.option("startingVersion", starting_version)
        elif starting_timestamp is not None:
            reader = reader.option("startingTimestamp", starting_timestamp)

        if ending_version is not None:
            reader = reader.option("endingVersion", ending_version)
        elif ending_timestamp is not None:
            reader = reader.option("endingTimestamp", ending_timestamp)

        return reader.load(table.config.path)
