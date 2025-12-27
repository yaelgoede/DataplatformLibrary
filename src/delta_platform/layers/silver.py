"""Silver layer implementation for cleaned and validated data."""

from typing import Optional, Callable, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp
from delta.tables import DeltaTable

from delta_platform.layers.base import BaseLayer


class SilverLayer(BaseLayer):
    """Silver layer for cleaned, validated, and conformed data."""

    def __init__(self, spark, layer_path: str, checkpoint_path: Optional[str] = None):
        """Initialize the Silver layer.

        Args:
            spark: Active SparkSession
            layer_path: Path for silver layer data
            checkpoint_path: Path for streaming checkpoints
        """
        super().__init__(spark, layer_path, "silver", checkpoint_path)

    def transform_from_bronze(
        self,
        bronze_table: str,
        silver_table: str,
        transformation_func: Callable[[DataFrame], DataFrame],
        mode: str = "append",
        partition_by: Optional[list[str]] = None,
        deduplicate_columns: Optional[list[str]] = None
    ) -> DataFrame:
        """Transform data from bronze to silver layer.

        Args:
            bronze_table: Source bronze table path or name
            silver_table: Target silver table name
            transformation_func: Function to transform the DataFrame
            mode: Write mode (append, overwrite)
            partition_by: Columns to partition by
            deduplicate_columns: Columns to use for deduplication

        Returns:
            Transformed DataFrame
        """
        # Read from bronze
        df = self.spark.read.format("delta").load(bronze_table)

        # Apply transformations
        transformed_df = transformation_func(df)

        # Add processing timestamp
        transformed_df = transformed_df.withColumn("_processed_timestamp", current_timestamp())

        # Deduplicate if specified
        if deduplicate_columns:
            transformed_df = self.deduplicate(transformed_df, deduplicate_columns)

        # Write to silver
        self.write_table(transformed_df, silver_table, mode=mode, partition_by=partition_by)

        return transformed_df

    def deduplicate(
        self,
        df: DataFrame,
        columns: list[str],
        order_by: Optional[str] = None,
        ascending: bool = False
    ) -> DataFrame:
        """Remove duplicate rows based on specified columns.

        Args:
            df: Input DataFrame
            columns: Columns to determine duplicates
            order_by: Column to order by before deduplication
            ascending: Sort order for order_by column

        Returns:
            Deduplicated DataFrame
        """
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number

        if order_by:
            window_spec = Window.partitionBy(*columns).orderBy(
                col(order_by).asc() if ascending else col(order_by).desc()
            )
            return df.withColumn("_row_num", row_number().over(window_spec)) \
                     .filter(col("_row_num") == 1) \
                     .drop("_row_num")
        else:
            return df.dropDuplicates(columns)

    def merge_updates(
        self,
        source_df: DataFrame,
        target_table: str,
        merge_keys: list[str],
        update_columns: Optional[list[str]] = None,
        insert_only: bool = False
    ) -> None:
        """Merge updates from source DataFrame into target table.

        Args:
            source_df: Source DataFrame with updates
            target_table: Target table name
            merge_keys: Columns to match on for merge
            update_columns: Columns to update (None = all columns)
            insert_only: If True, only insert new records, don't update
        """
        if not self.table_exists(target_table):
            # If table doesn't exist, create it
            self.write_table(source_df, target_table, mode="overwrite")
            return

        target = self.get_delta_table(target_table)

        # Build merge condition
        merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])

        merge_builder = target.alias("target").merge(
            source_df.alias("source"),
            merge_condition
        )

        if not insert_only:
            # Determine which columns to update
            if update_columns is None:
                update_columns = [c for c in source_df.columns if c not in merge_keys]

            update_dict = {col: f"source.{col}" for col in update_columns}
            merge_builder = merge_builder.whenMatchedUpdate(set=update_dict)

        # Insert when not matched
        merge_builder = merge_builder.whenNotMatchedInsertAll()

        merge_builder.execute()

    def apply_scd_type2(
        self,
        source_df: DataFrame,
        target_table: str,
        natural_keys: list[str],
        start_date_col: str = "effective_start_date",
        end_date_col: str = "effective_end_date",
        current_flag_col: str = "is_current"
    ) -> None:
        """Apply Slowly Changing Dimension Type 2 logic.

        Args:
            source_df: Source DataFrame with new/updated records
            target_table: Target table name
            natural_keys: Business keys to identify records
            start_date_col: Column name for start date
            end_date_col: Column name for end date
            current_flag_col: Column name for current flag
        """
        from pyspark.sql.functions import lit, when

        # Add SCD2 columns to source
        source_with_scd = source_df \
            .withColumn(start_date_col, current_timestamp()) \
            .withColumn(end_date_col, lit(None).cast("timestamp")) \
            .withColumn(current_flag_col, lit(True))

        if not self.table_exists(target_table):
            self.write_table(source_with_scd, target_table, mode="overwrite")
            return

        target = self.get_delta_table(target_table)

        # Build merge condition on natural keys
        merge_condition = " AND ".join([
            f"target.{key} = source.{key}" for key in natural_keys
        ]) + f" AND target.{current_flag_col} = true"

        # Merge logic
        target.alias("target").merge(
            source_with_scd.alias("source"),
            merge_condition
        ).whenMatchedUpdate(
            set={
                end_date_col: current_timestamp(),
                current_flag_col: lit(False)
            }
        ).whenNotMatchedInsertAll().execute()

        # Insert new versions
        self.write_table(source_with_scd, target_table, mode="append")

    def clean_data(
        self,
        df: DataFrame,
        drop_duplicates: bool = True,
        drop_nulls: Optional[list[str]] = None,
        trim_strings: bool = True
    ) -> DataFrame:
        """Apply common data cleaning operations.

        Args:
            df: Input DataFrame
            drop_duplicates: Whether to drop duplicate rows
            drop_nulls: Columns that should not have null values
            trim_strings: Whether to trim string columns

        Returns:
            Cleaned DataFrame
        """
        from pyspark.sql.functions import trim
        from pyspark.sql.types import StringType

        result_df = df

        # Drop duplicates
        if drop_duplicates:
            result_df = result_df.dropDuplicates()

        # Drop rows with nulls in specified columns
        if drop_nulls:
            result_df = result_df.dropna(subset=drop_nulls)

        # Trim string columns
        if trim_strings:
            string_cols = [field.name for field in result_df.schema.fields
                          if isinstance(field.dataType, StringType)]
            for col_name in string_cols:
                result_df = result_df.withColumn(col_name, trim(col(col_name)))

        return result_df

    def process(
        self,
        bronze_table: str,
        silver_table: str,
        transformation_func: Callable[[DataFrame], DataFrame],
        **kwargs
    ) -> DataFrame:
        """Generic process method for silver layer.

        Args:
            bronze_table: Source bronze table
            silver_table: Target silver table
            transformation_func: Transformation function
            **kwargs: Additional arguments

        Returns:
            Transformed DataFrame
        """
        return self.transform_from_bronze(
            bronze_table,
            silver_table,
            transformation_func,
            **kwargs
        )
