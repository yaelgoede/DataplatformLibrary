"""Bronze layer implementation for raw data ingestion."""

from typing import Optional, Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit

from delta_platform.layers.base import BaseLayer
from delta_platform.ingestion.readers import (
    IngestConfig,
    read_json,
    read_xml,
    read_parquet,
    read_csv
)


class BronzeLayer(BaseLayer):
    """Bronze layer for ingesting raw data from various sources."""

    def __init__(self, spark, layer_path: str, checkpoint_path: Optional[str] = None):
        """Initialize the Bronze layer.

        Args:
            spark: Active SparkSession
            layer_path: Path for bronze layer data
            checkpoint_path: Path for streaming checkpoints
        """
        super().__init__(spark, layer_path, "bronze", checkpoint_path)

    def ingest_json(
        self,
        source_path: str,
        table_name: str,
        multiline: bool = False,
        mode: str = "append",
        partition_by: Optional[list[str]] = None,
        options: Optional[Dict[str, Any]] = None
    ) -> DataFrame:
        """Ingest JSON data into the bronze layer.

        Args:
            source_path: Path to JSON files
            table_name: Target table name
            multiline: Whether JSON is multiline
            mode: Write mode (append, overwrite)
            partition_by: Columns to partition by
            options: Additional read options

        Returns:
            DataFrame that was ingested
        """
        config = IngestConfig(
            source_path=source_path,
            add_metadata=True,
            options=options or {}
        )

        df = read_json(self.spark, config, multiline=multiline)
        self.write_table(df, table_name, mode=mode, partition_by=partition_by)

        return df

    def ingest_xml(
        self,
        source_path: str,
        table_name: str,
        row_tag: str = "row",
        mode: str = "append",
        partition_by: Optional[list[str]] = None,
        options: Optional[Dict[str, Any]] = None
    ) -> DataFrame:
        """Ingest XML data into the bronze layer.

        Args:
            source_path: Path to XML files
            table_name: Target table name
            row_tag: XML row tag name
            mode: Write mode (append, overwrite)
            partition_by: Columns to partition by
            options: Additional read options

        Returns:
            DataFrame that was ingested
        """
        config = IngestConfig(
            source_path=source_path,
            add_metadata=True,
            options=options or {}
        )

        df = read_xml(self.spark, config, row_tag=row_tag)
        self.write_table(df, table_name, mode=mode, partition_by=partition_by)

        return df

    def ingest_parquet(
        self,
        source_path: str,
        table_name: str,
        mode: str = "append",
        partition_by: Optional[list[str]] = None,
        options: Optional[Dict[str, Any]] = None
    ) -> DataFrame:
        """Ingest Parquet data into the bronze layer.

        Args:
            source_path: Path to Parquet files
            table_name: Target table name
            mode: Write mode (append, overwrite)
            partition_by: Columns to partition by
            options: Additional read options

        Returns:
            DataFrame that was ingested
        """
        config = IngestConfig(
            source_path=source_path,
            add_metadata=True,
            options=options or {}
        )

        df = read_parquet(self.spark, config)
        self.write_table(df, table_name, mode=mode, partition_by=partition_by)

        return df

    def ingest_csv(
        self,
        source_path: str,
        table_name: str,
        header: bool = True,
        infer_schema: bool = True,
        delimiter: str = ",",
        mode: str = "append",
        partition_by: Optional[list[str]] = None,
        options: Optional[Dict[str, Any]] = None
    ) -> DataFrame:
        """Ingest CSV data into the bronze layer.

        Args:
            source_path: Path to CSV files
            table_name: Target table name
            header: Whether CSV has header
            infer_schema: Whether to infer schema
            delimiter: CSV delimiter
            mode: Write mode (append, overwrite)
            partition_by: Columns to partition by
            options: Additional read options

        Returns:
            DataFrame that was ingested
        """
        config = IngestConfig(
            source_path=source_path,
            add_metadata=True,
            options=options or {}
        )

        df = read_csv(self.spark, config, header=header, infer_schema=infer_schema, delimiter=delimiter)
        self.write_table(df, table_name, mode=mode, partition_by=partition_by)

        return df

    def ingest_streaming(
        self,
        source_path: str,
        table_name: str,
        format: str = "json",
        trigger_interval: str = "10 seconds",
        checkpoint_suffix: str = "",
        options: Optional[Dict[str, Any]] = None
    ) -> None:
        """Ingest data using Spark Structured Streaming.

        Args:
            source_path: Path to source files
            table_name: Target table name
            format: Source format (json, parquet, csv, etc.)
            trigger_interval: Stream trigger interval
            checkpoint_suffix: Suffix for checkpoint path
            options: Additional read options
        """
        reader = self.spark.readStream.format(format)

        if options:
            for key, value in options.items():
                reader = reader.option(key, value)

        stream_df = reader.load(source_path)

        checkpoint_location = f"{self.checkpoint_path}/{table_name}{checkpoint_suffix}"

        query = stream_df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", checkpoint_location) \
            .trigger(processingTime=trigger_interval) \
            .start(self.table_path(table_name))

        return query

    def process(
        self,
        source_path: str,
        table_name: str,
        format: str = "json",
        **kwargs
    ) -> DataFrame:
        """Generic process method for bronze layer.

        Args:
            source_path: Path to source data
            table_name: Target table name
            format: Data format (json, xml, parquet, csv)
            **kwargs: Additional arguments for specific ingest method

        Returns:
            DataFrame that was ingested
        """
        format_methods = {
            "json": self.ingest_json,
            "xml": self.ingest_xml,
            "parquet": self.ingest_parquet,
            "csv": self.ingest_csv
        }

        if format not in format_methods:
            raise ValueError(f"Unsupported format: {format}. Supported formats: {list(format_methods.keys())}")

        return format_methods[format](source_path, table_name, **kwargs)
