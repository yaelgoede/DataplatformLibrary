"""File readers for various data formats."""

from dataclasses import dataclass, field
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, input_file_name


@dataclass
class IngestConfig:
    """Configuration for data ingestion."""

    source_path: str
    add_metadata: bool = True
    schema: Optional[str] = None
    options: Dict[str, Any] = field(default_factory=dict)


def _add_ingestion_metadata(df: DataFrame) -> DataFrame:
    """Add ingestion metadata columns to a DataFrame.

    Args:
        df: Source DataFrame

    Returns:
        DataFrame with metadata columns added
    """
    return df.withColumn("_ingestion_timestamp", current_timestamp()) \
             .withColumn("_source_file", input_file_name())


def read_json(
    spark: SparkSession,
    config: IngestConfig,
    multiline: bool = False
) -> DataFrame:
    """Read JSON files into a DataFrame.

    Args:
        spark: Active SparkSession
        config: Ingestion configuration
        multiline: Whether to read multiline JSON

    Returns:
        DataFrame containing JSON data
    """
    reader = spark.read.format("json")

    if multiline:
        reader = reader.option("multiline", "true")

    if config.schema:
        reader = reader.schema(config.schema)

    for key, value in config.options.items():
        reader = reader.option(key, value)

    df = reader.load(config.source_path)

    if config.add_metadata:
        df = _add_ingestion_metadata(df)

    return df


def read_xml(
    spark: SparkSession,
    config: IngestConfig,
    row_tag: str = "row"
) -> DataFrame:
    """Read XML files into a DataFrame.

    Args:
        spark: Active SparkSession
        config: Ingestion configuration
        row_tag: XML row tag name

    Returns:
        DataFrame containing XML data
    """
    reader = spark.read.format("xml").option("rowTag", row_tag)

    if config.schema:
        reader = reader.schema(config.schema)

    for key, value in config.options.items():
        reader = reader.option(key, value)

    df = reader.load(config.source_path)

    if config.add_metadata:
        df = _add_ingestion_metadata(df)

    return df


def read_parquet(
    spark: SparkSession,
    config: IngestConfig
) -> DataFrame:
    """Read Parquet files into a DataFrame.

    Args:
        spark: Active SparkSession
        config: Ingestion configuration

    Returns:
        DataFrame containing Parquet data
    """
    reader = spark.read.format("parquet")

    if config.schema:
        reader = reader.schema(config.schema)

    for key, value in config.options.items():
        reader = reader.option(key, value)

    df = reader.load(config.source_path)

    if config.add_metadata:
        df = _add_ingestion_metadata(df)

    return df


def read_csv(
    spark: SparkSession,
    config: IngestConfig,
    header: bool = True,
    infer_schema: bool = True,
    delimiter: str = ","
) -> DataFrame:
    """Read CSV files into a DataFrame.

    Args:
        spark: Active SparkSession
        config: Ingestion configuration
        header: Whether CSV has header row
        infer_schema: Whether to infer schema
        delimiter: CSV delimiter

    Returns:
        DataFrame containing CSV data
    """
    reader = spark.read.format("csv") \
        .option("header", str(header).lower()) \
        .option("inferSchema", str(infer_schema).lower()) \
        .option("delimiter", delimiter)

    if config.schema:
        reader = reader.schema(config.schema)

    for key, value in config.options.items():
        reader = reader.option(key, value)

    df = reader.load(config.source_path)

    if config.add_metadata:
        df = _add_ingestion_metadata(df)

    return df
