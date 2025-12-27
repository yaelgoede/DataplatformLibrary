"""Helper utilities for Delta Platform."""

from typing import Dict, Any, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType


def create_sample_data(
    spark: SparkSession,
    schema: StructType,
    data: List[tuple]
) -> DataFrame:
    """Create a sample DataFrame from schema and data.

    Args:
        spark: SparkSession
        schema: DataFrame schema
        data: List of tuples representing rows

    Returns:
        DataFrame with sample data
    """
    return spark.createDataFrame(data, schema)


def validate_schema(
    df: DataFrame,
    required_columns: List[str],
    raise_error: bool = True
) -> bool:
    """Validate that a DataFrame has required columns.

    Args:
        df: DataFrame to validate
        required_columns: List of required column names
        raise_error: Whether to raise error on validation failure

    Returns:
        True if valid, False otherwise

    Raises:
        ValueError: If validation fails and raise_error is True
    """
    df_columns = set(df.columns)
    required_set = set(required_columns)
    missing = required_set - df_columns

    if missing:
        if raise_error:
            raise ValueError(f"Missing required columns: {missing}")
        return False

    return True


def get_table_statistics(df: DataFrame) -> Dict[str, Any]:
    """Get basic statistics about a DataFrame.

    Args:
        df: DataFrame to analyze

    Returns:
        Dict with statistics
    """
    return {
        "row_count": df.count(),
        "column_count": len(df.columns),
        "columns": df.columns,
        "schema": df.schema.simpleString(),
        "size_bytes": df.rdd.map(lambda row: len(str(row))).sum()
    }
