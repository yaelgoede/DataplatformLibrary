"""Transformation strategies using the Strategy pattern."""

from abc import ABC, abstractmethod
from typing import Callable, List, Optional, Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, current_timestamp
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


class TransformationStrategy(ABC):
    """
    Abstract base class for transformation strategies.

    This implements the Strategy pattern, allowing different transformation
    algorithms to be used interchangeably.
    """

    @abstractmethod
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """Apply transformation to the DataFrame.

        Args:
            df: Input DataFrame
            **kwargs: Additional transformation parameters

        Returns:
            Transformed DataFrame
        """
        pass


class CleaningStrategy(TransformationStrategy):
    """Strategy for cleaning data.

    This strategy applies common data cleaning operations like:
    - Removing duplicates
    - Handling null values
    - Trimming strings
    - Type conversions
    """

    def __init__(
        self,
        drop_duplicates: bool = True,
        drop_null_columns: Optional[List[str]] = None,
        trim_strings: bool = True,
        fill_null_values: Optional[Dict[str, Any]] = None
    ):
        """Initialize cleaning strategy.

        Args:
            drop_duplicates: Whether to drop duplicate rows
            drop_null_columns: Columns that should not have null values
            trim_strings: Whether to trim string columns
            fill_null_values: Dict of {column: fill_value} for null handling
        """
        self.drop_duplicates = drop_duplicates
        self.drop_null_columns = drop_null_columns or []
        self.trim_strings = trim_strings
        self.fill_null_values = fill_null_values or {}

    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """Apply cleaning transformations.

        Args:
            df: Input DataFrame
            **kwargs: Additional parameters

        Returns:
            Cleaned DataFrame
        """
        result_df = df

        # Drop duplicates
        if self.drop_duplicates:
            result_df = result_df.dropDuplicates()

        # Drop rows with nulls in specified columns
        if self.drop_null_columns:
            result_df = result_df.dropna(subset=self.drop_null_columns)

        # Fill null values
        if self.fill_null_values:
            result_df = result_df.fillna(self.fill_null_values)

        # Trim string columns
        if self.trim_strings:
            string_cols = [
                field.name for field in result_df.schema.fields
                if isinstance(field.dataType, StringType)
            ]
            for col_name in string_cols:
                result_df = result_df.withColumn(col_name, trim(col(col_name)))

        return result_df


class DeduplicationStrategy(TransformationStrategy):
    """Strategy for deduplicating data based on specific columns."""

    def __init__(
        self,
        dedupe_columns: List[str],
        order_by: Optional[str] = None,
        ascending: bool = False,
        keep: str = "last"
    ):
        """Initialize deduplication strategy.

        Args:
            dedupe_columns: Columns to determine duplicates
            order_by: Column to order by before deduplication
            ascending: Sort order for order_by column
            keep: Which record to keep ('first' or 'last')
        """
        self.dedupe_columns = dedupe_columns
        self.order_by = order_by
        self.ascending = ascending
        self.keep = keep

    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """Apply deduplication.

        Args:
            df: Input DataFrame
            **kwargs: Additional parameters

        Returns:
            Deduplicated DataFrame
        """
        if self.order_by:
            # Window-based deduplication
            window_spec = Window.partitionBy(*self.dedupe_columns).orderBy(
                col(self.order_by).asc() if self.ascending else col(self.order_by).desc()
            )

            if self.keep == "last":
                # Keep the last record (highest row number)
                return df.withColumn("_row_num", row_number().over(window_spec)) \
                    .filter(col("_row_num") == 1) \
                    .drop("_row_num")
            else:
                # Keep the first record
                return df.withColumn("_row_num", row_number().over(window_spec)) \
                    .filter(col("_row_num") == 1) \
                    .drop("_row_num")
        else:
            # Simple deduplication
            return df.dropDuplicates(self.dedupe_columns)


class FilterStrategy(TransformationStrategy):
    """Strategy for filtering data based on conditions."""

    def __init__(self, filter_condition: str):
        """Initialize filter strategy.

        Args:
            filter_condition: SQL-like filter condition
        """
        self.filter_condition = filter_condition

    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """Apply filter.

        Args:
            df: Input DataFrame
            **kwargs: Additional parameters

        Returns:
            Filtered DataFrame
        """
        return df.filter(self.filter_condition)


class AggregationStrategy(TransformationStrategy):
    """Strategy for aggregating data."""

    def __init__(
        self,
        group_by: List[str],
        aggregations: Dict[str, str]
    ):
        """Initialize aggregation strategy.

        Args:
            group_by: Columns to group by
            aggregations: Dict of {column: agg_func} (e.g., {"amount": "sum"})
        """
        self.group_by = group_by
        self.aggregations = aggregations

    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """Apply aggregation.

        Args:
            df: Input DataFrame
            **kwargs: Additional parameters

        Returns:
            Aggregated DataFrame
        """
        from pyspark.sql.functions import sum, avg, count, min, max, first, last

        agg_funcs = {
            "sum": sum,
            "avg": avg,
            "count": count,
            "min": min,
            "max": max,
            "first": first,
            "last": last
        }

        agg_exprs = []
        for column, agg_type in self.aggregations.items():
            if agg_type in agg_funcs:
                agg_exprs.append(
                    agg_funcs[agg_type](col(column)).alias(f"{column}_{agg_type}")
                )

        return df.groupBy(*self.group_by).agg(*agg_exprs)


class CustomStrategy(TransformationStrategy):
    """Strategy for custom user-defined transformations."""

    def __init__(self, transform_func: Callable[[DataFrame], DataFrame]):
        """Initialize custom strategy.

        Args:
            transform_func: Custom transformation function
        """
        self.transform_func = transform_func

    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """Apply custom transformation.

        Args:
            df: Input DataFrame
            **kwargs: Additional parameters passed to transform_func

        Returns:
            Transformed DataFrame
        """
        return self.transform_func(df, **kwargs)


class ChainStrategy(TransformationStrategy):
    """Strategy for chaining multiple transformations."""

    def __init__(self, strategies: List[TransformationStrategy]):
        """Initialize chain strategy.

        Args:
            strategies: List of strategies to apply in sequence
        """
        self.strategies = strategies

    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """Apply all strategies in sequence.

        Args:
            df: Input DataFrame
            **kwargs: Additional parameters

        Returns:
            Transformed DataFrame
        """
        result_df = df
        for strategy in self.strategies:
            result_df = strategy.transform(result_df, **kwargs)
        return result_df
