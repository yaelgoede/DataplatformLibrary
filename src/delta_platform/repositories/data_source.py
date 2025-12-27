"""Repository pattern for data source access."""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession, DataFrame


class DataSourceRepository(ABC):
    """
    Abstract repository for accessing data sources.

    This implements the Repository pattern, abstracting data access
    and providing a clean interface for different data sources.
    """

    def __init__(self, spark: SparkSession, source_path: str):
        """Initialize the repository.

        Args:
            spark: Active SparkSession
            source_path: Path to data source
        """
        self.spark = spark
        self.source_path = source_path

    @abstractmethod
    def read(self, **options) -> DataFrame:
        """Read data from the source.

        Args:
            **options: Reader options

        Returns:
            DataFrame containing source data
        """
        pass

    def read_stream(self, **options) -> DataFrame:
        """Read data as a stream.

        Args:
            **options: Reader options

        Returns:
            Streaming DataFrame
        """
        raise NotImplementedError("Streaming not implemented for this source")


class JsonDataSource(DataSourceRepository):
    """Repository for JSON data sources."""

    def read(
        self,
        multiline: bool = False,
        schema: Optional[str] = None,
        **options
    ) -> DataFrame:
        """Read JSON data.

        Args:
            multiline: Whether JSON is multiline
            schema: Optional schema
            **options: Additional reader options

        Returns:
            DataFrame containing JSON data
        """
        reader = self.spark.read.format("json")

        if multiline:
            reader = reader.option("multiline", "true")

        if schema:
            reader = reader.schema(schema)

        for key, value in options.items():
            reader = reader.option(key, value)

        return reader.load(self.source_path)

    def read_stream(
        self,
        multiline: bool = False,
        schema: Optional[str] = None,
        **options
    ) -> DataFrame:
        """Read JSON data as a stream.

        Args:
            multiline: Whether JSON is multiline
            schema: Optional schema
            **options: Additional reader options

        Returns:
            Streaming DataFrame
        """
        reader = self.spark.readStream.format("json")

        if multiline:
            reader = reader.option("multiline", "true")

        if schema:
            reader = reader.schema(schema)

        for key, value in options.items():
            reader = reader.option(key, value)

        return reader.load(self.source_path)


class XmlDataSource(DataSourceRepository):
    """Repository for XML data sources."""

    def read(
        self,
        row_tag: str = "row",
        schema: Optional[str] = None,
        **options
    ) -> DataFrame:
        """Read XML data.

        Args:
            row_tag: XML row tag name
            schema: Optional schema
            **options: Additional reader options

        Returns:
            DataFrame containing XML data
        """
        reader = self.spark.read.format("xml").option("rowTag", row_tag)

        if schema:
            reader = reader.schema(schema)

        for key, value in options.items():
            reader = reader.option(key, value)

        return reader.load(self.source_path)


class ParquetDataSource(DataSourceRepository):
    """Repository for Parquet data sources."""

    def read(self, schema: Optional[str] = None, **options) -> DataFrame:
        """Read Parquet data.

        Args:
            schema: Optional schema
            **options: Additional reader options

        Returns:
            DataFrame containing Parquet data
        """
        reader = self.spark.read.format("parquet")

        if schema:
            reader = reader.schema(schema)

        for key, value in options.items():
            reader = reader.option(key, value)

        return reader.load(self.source_path)

    def read_stream(self, schema: Optional[str] = None, **options) -> DataFrame:
        """Read Parquet data as a stream.

        Args:
            schema: Optional schema
            **options: Additional reader options

        Returns:
            Streaming DataFrame
        """
        reader = self.spark.readStream.format("parquet")

        if schema:
            reader = reader.schema(schema)

        for key, value in options.items():
            reader = reader.option(key, value)

        return reader.load(self.source_path)


class CsvDataSource(DataSourceRepository):
    """Repository for CSV data sources."""

    def read(
        self,
        header: bool = True,
        infer_schema: bool = True,
        delimiter: str = ",",
        schema: Optional[str] = None,
        **options
    ) -> DataFrame:
        """Read CSV data.

        Args:
            header: Whether CSV has header
            infer_schema: Whether to infer schema
            delimiter: CSV delimiter
            schema: Optional schema
            **options: Additional reader options

        Returns:
            DataFrame containing CSV data
        """
        reader = self.spark.read.format("csv") \
            .option("header", str(header).lower()) \
            .option("inferSchema", str(infer_schema).lower()) \
            .option("delimiter", delimiter)

        if schema:
            reader = reader.schema(schema)

        for key, value in options.items():
            reader = reader.option(key, value)

        return reader.load(self.source_path)

    def read_stream(
        self,
        header: bool = True,
        infer_schema: bool = False,
        delimiter: str = ",",
        schema: Optional[str] = None,
        **options
    ) -> DataFrame:
        """Read CSV data as a stream.

        Args:
            header: Whether CSV has header
            infer_schema: Whether to infer schema (usually False for streaming)
            delimiter: CSV delimiter
            schema: Optional schema (recommended for streaming)
            **options: Additional reader options

        Returns:
            Streaming DataFrame
        """
        reader = self.spark.readStream.format("csv") \
            .option("header", str(header).lower()) \
            .option("inferSchema", str(infer_schema).lower()) \
            .option("delimiter", delimiter)

        if schema:
            reader = reader.schema(schema)

        for key, value in options.items():
            reader = reader.option(key, value)

        return reader.load(self.source_path)


class DeltaDataSource(DataSourceRepository):
    """Repository for Delta Lake data sources."""

    def read(self, **options) -> DataFrame:
        """Read Delta data.

        Args:
            **options: Additional reader options

        Returns:
            DataFrame containing Delta data
        """
        reader = self.spark.read.format("delta")

        for key, value in options.items():
            reader = reader.option(key, value)

        return reader.load(self.source_path)

    def read_stream(self, **options) -> DataFrame:
        """Read Delta data as a stream.

        Args:
            **options: Additional reader options

        Returns:
            Streaming DataFrame
        """
        reader = self.spark.readStream.format("delta")

        for key, value in options.items():
            reader = reader.option(key, value)

        return reader.load(self.source_path)
