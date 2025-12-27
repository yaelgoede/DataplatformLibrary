"""CDC transformation strategies."""

from enum import Enum
from typing import List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

from delta_platform.strategies.transformation import TransformationStrategy


class CDCOperation(Enum):
    """CDC operation types."""

    INSERT = "I"
    UPDATE = "U"
    DELETE = "D"
    SNAPSHOT = "S"  # Full snapshot/initial load


class CDCStrategy(TransformationStrategy):
    """
    Strategy for processing Change Data Capture (CDC) data.

    This strategy handles CDC events with operations like INSERT, UPDATE, DELETE
    and prepares them for merging into target tables.
    """

    def __init__(
        self,
        operation_column: str = "op",
        sequence_column: Optional[str] = "seq",
        timestamp_column: Optional[str] = "ts",
        keep_latest_only: bool = True
    ):
        """Initialize CDC strategy.

        Args:
            operation_column: Column containing operation type (I/U/D)
            sequence_column: Column for ordering changes (if multiple changes per key)
            timestamp_column: Timestamp column for changes
            keep_latest_only: Whether to keep only the latest change per key
        """
        self.operation_column = operation_column
        self.sequence_column = sequence_column
        self.timestamp_column = timestamp_column
        self.keep_latest_only = keep_latest_only

    def transform(self, df: DataFrame, primary_keys: Optional[List[str]] = None, **kwargs) -> DataFrame:
        """Process CDC events.

        Args:
            df: CDC DataFrame
            primary_keys: List of primary key columns for deduplication
            **kwargs: Additional parameters

        Returns:
            Processed CDC DataFrame
        """
        result_df = df

        # Deduplicate to keep only latest change per key
        if self.keep_latest_only and primary_keys:
            order_columns = []

            if self.sequence_column:
                order_columns.append(col(self.sequence_column).desc())
            if self.timestamp_column:
                order_columns.append(col(self.timestamp_column).desc())

            if order_columns:
                window_spec = Window.partitionBy(*primary_keys).orderBy(*order_columns)

                result_df = result_df \
                    .withColumn("_row_num", row_number().over(window_spec)) \
                    .filter(col("_row_num") == 1) \
                    .drop("_row_num")

        return result_df

    def filter_by_operation(self, df: DataFrame, operation: CDCOperation) -> DataFrame:
        """Filter CDC data by operation type.

        Args:
            df: CDC DataFrame
            operation: Operation type to filter

        Returns:
            Filtered DataFrame
        """
        return df.filter(col(self.operation_column) == operation.value)


class MergeFromCDCStrategy(TransformationStrategy):
    """Strategy for merging CDC data into target tables."""

    def __init__(
        self,
        cdc_strategy: CDCStrategy,
        delete_operation: str = CDCOperation.DELETE.value
    ):
        """Initialize merge strategy.

        Args:
            cdc_strategy: CDC processing strategy
            delete_operation: Operation value indicating deletion
        """
        self.cdc_strategy = cdc_strategy
        self.delete_operation = delete_operation

    def transform(self, df: DataFrame, primary_keys: List[str], **kwargs) -> DataFrame:
        """Prepare CDC data for merge operation.

        Args:
            df: CDC DataFrame
            primary_keys: Primary key columns
            **kwargs: Additional parameters

        Returns:
            DataFrame ready for merge
        """
        # Process CDC events
        processed_df = self.cdc_strategy.transform(df, primary_keys=primary_keys)

        # Mark records for deletion
        processed_df = processed_df.withColumn(
            "_is_deleted",
            col(self.cdc_strategy.operation_column) == self.delete_operation
        )

        return processed_df
