"""Gold layer implementation for business-level aggregations."""

from typing import Optional, Callable, Dict, List, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp

from delta_platform.layers.base import BaseLayer


class GoldLayer(BaseLayer):
    """Gold layer for business-level aggregations and analytics-ready data."""

    def __init__(self, spark, layer_path: str, checkpoint_path: Optional[str] = None):
        """Initialize the Gold layer.

        Args:
            spark: Active SparkSession
            layer_path: Path for gold layer data
            checkpoint_path: Path for streaming checkpoints
        """
        super().__init__(spark, layer_path, "gold", checkpoint_path)

    def create_aggregate(
        self,
        source_tables: List[str],
        target_table: str,
        aggregation_func: Callable[[List[DataFrame]], DataFrame],
        mode: str = "overwrite",
        partition_by: Optional[list[str]] = None
    ) -> DataFrame:
        """Create aggregated data in the gold layer.

        Args:
            source_tables: List of source table paths
            target_table: Target table name
            aggregation_func: Function to aggregate DataFrames
            mode: Write mode (append, overwrite)
            partition_by: Columns to partition by

        Returns:
            Aggregated DataFrame
        """
        # Read source tables
        source_dfs = [
            self.spark.read.format("delta").load(table)
            for table in source_tables
        ]

        # Apply aggregation
        aggregated_df = aggregation_func(source_dfs)

        # Add processing timestamp
        aggregated_df = aggregated_df.withColumn("_aggregated_timestamp", current_timestamp())

        # Write to gold
        self.write_table(aggregated_df, target_table, mode=mode, partition_by=partition_by)

        return aggregated_df

    def create_dimension(
        self,
        source_table: str,
        dimension_table: str,
        dimension_columns: list[str],
        surrogate_key: Optional[str] = None,
        mode: str = "overwrite"
    ) -> DataFrame:
        """Create a dimension table from source data.

        Args:
            source_table: Source table path
            dimension_table: Target dimension table name
            dimension_columns: Columns to include in dimension
            surrogate_key: Name for surrogate key column
            mode: Write mode

        Returns:
            Dimension DataFrame
        """
        from pyspark.sql.functions import monotonically_increasing_id

        # Read source
        df = self.spark.read.format("delta").load(source_table)

        # Select dimension columns and remove duplicates
        dim_df = df.select(*dimension_columns).distinct()

        # Add surrogate key if specified
        if surrogate_key:
            dim_df = dim_df.withColumn(surrogate_key, monotonically_increasing_id())

        # Write dimension table
        self.write_table(dim_df, dimension_table, mode=mode)

        return dim_df

    def create_fact_table(
        self,
        source_tables: Dict[str, str],
        fact_table: str,
        join_func: Callable[[Dict[str, DataFrame]], DataFrame],
        measures: list[str],
        dimensions: list[str],
        mode: str = "overwrite",
        partition_by: Optional[list[str]] = None
    ) -> DataFrame:
        """Create a fact table by joining multiple sources.

        Args:
            source_tables: Dict of {alias: table_path}
            fact_table: Target fact table name
            join_func: Function to join source DataFrames
            measures: Measure columns to include
            dimensions: Dimension key columns to include
            mode: Write mode
            partition_by: Columns to partition by

        Returns:
            Fact DataFrame
        """
        # Read source tables into dict
        source_dfs = {
            alias: self.spark.read.format("delta").load(path)
            for alias, path in source_tables.items()
        }

        # Apply joins
        joined_df = join_func(source_dfs)

        # Select measures and dimensions
        fact_df = joined_df.select(*(dimensions + measures))

        # Add processing timestamp
        fact_df = fact_df.withColumn("_fact_timestamp", current_timestamp())

        # Write fact table
        self.write_table(fact_df, fact_table, mode=mode, partition_by=partition_by)

        return fact_df

    def create_summary(
        self,
        source_table: str,
        summary_table: str,
        group_by: list[str],
        aggregations: Dict[str, str],
        mode: str = "overwrite",
        partition_by: Optional[list[str]] = None
    ) -> DataFrame:
        """Create a summary/aggregation table.

        Args:
            source_table: Source table path
            summary_table: Target summary table name
            group_by: Columns to group by
            aggregations: Dict of {column: aggregation} (e.g., {"amount": "sum"})
            mode: Write mode
            partition_by: Columns to partition by

        Returns:
            Summary DataFrame
        """
        from pyspark.sql.functions import sum, avg, count, min, max

        agg_funcs = {
            "sum": sum,
            "avg": avg,
            "count": count,
            "min": min,
            "max": max
        }

        # Read source
        df = self.spark.read.format("delta").load(source_table)

        # Build aggregations
        agg_exprs = []
        for column, agg_type in aggregations.items():
            if agg_type in agg_funcs:
                agg_exprs.append(
                    agg_funcs[agg_type](col(column)).alias(f"{column}_{agg_type}")
                )

        # Apply grouping and aggregation
        summary_df = df.groupBy(*group_by).agg(*agg_exprs)

        # Add processing timestamp
        summary_df = summary_df.withColumn("_summary_timestamp", current_timestamp())

        # Write summary table
        self.write_table(summary_df, summary_table, mode=mode, partition_by=partition_by)

        return summary_df

    def create_wide_table(
        self,
        source_tables: Dict[str, str],
        wide_table: str,
        join_keys: list[str],
        join_type: str = "inner",
        mode: str = "overwrite",
        partition_by: Optional[list[str]] = None
    ) -> DataFrame:
        """Create a denormalized wide table by joining multiple sources.

        Args:
            source_tables: Dict of {alias: table_path}
            wide_table: Target wide table name
            join_keys: Keys to join on
            join_type: Type of join (inner, left, right, outer)
            mode: Write mode
            partition_by: Columns to partition by

        Returns:
            Wide DataFrame
        """
        # Read first table
        first_alias = list(source_tables.keys())[0]
        result_df = self.spark.read.format("delta").load(source_tables[first_alias])

        # Join remaining tables
        for alias, table_path in list(source_tables.items())[1:]:
            df = self.spark.read.format("delta").load(table_path)
            result_df = result_df.join(df, on=join_keys, how=join_type)

        # Add processing timestamp
        result_df = result_df.withColumn("_wide_table_timestamp", current_timestamp())

        # Write wide table
        self.write_table(result_df, wide_table, mode=mode, partition_by=partition_by)

        return result_df

    def create_time_series(
        self,
        source_table: str,
        time_series_table: str,
        timestamp_column: str,
        group_by: list[str],
        metrics: Dict[str, str],
        time_grain: str = "day",
        mode: str = "overwrite"
    ) -> DataFrame:
        """Create a time-series aggregated table.

        Args:
            source_table: Source table path
            time_series_table: Target time series table name
            timestamp_column: Timestamp column to aggregate by
            group_by: Additional columns to group by
            metrics: Dict of {column: aggregation}
            time_grain: Time granularity (day, hour, month, year)
            mode: Write mode

        Returns:
            Time series DataFrame
        """
        from pyspark.sql.functions import date_trunc, sum, avg, count

        agg_funcs = {"sum": sum, "avg": avg, "count": count}

        # Read source
        df = self.spark.read.format("delta").load(source_table)

        # Truncate timestamp to desired grain
        df = df.withColumn("time_period", date_trunc(time_grain, col(timestamp_column)))

        # Build aggregations
        agg_exprs = []
        for column, agg_type in metrics.items():
            if agg_type in agg_funcs:
                agg_exprs.append(
                    agg_funcs[agg_type](col(column)).alias(f"{column}_{agg_type}")
                )

        # Group and aggregate
        ts_df = df.groupBy("time_period", *group_by).agg(*agg_exprs)

        # Write time series table
        self.write_table(ts_df, time_series_table, mode=mode, partition_by=["time_period"])

        return ts_df

    def process(
        self,
        source_tables: List[str],
        target_table: str,
        aggregation_func: Callable[[List[DataFrame]], DataFrame],
        **kwargs
    ) -> DataFrame:
        """Generic process method for gold layer.

        Args:
            source_tables: List of source tables
            target_table: Target table name
            aggregation_func: Aggregation function
            **kwargs: Additional arguments

        Returns:
            Aggregated DataFrame
        """
        return self.create_aggregate(
            source_tables,
            target_table,
            aggregation_func,
            **kwargs
        )
