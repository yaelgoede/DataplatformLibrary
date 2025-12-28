"""Streaming table implementation."""

from typing import Optional, Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

from delta_platform.core.table import DeltaTable
from delta_platform.streaming.trigger import TriggerConfig, TriggerType


class StreamingTable(DeltaTable):
    """
    Extended Delta table with streaming capabilities.

    Inherits from DeltaTable and adds streaming-specific operations.
    """

    def write_stream(
        self,
        stream_df: DataFrame,
        trigger: Optional[TriggerConfig] = None,
        output_mode: str = "append",
        checkpoint_location: Optional[str] = None,
        options: Optional[Dict[str, str]] = None
    ) -> StreamingQuery:
        """Write streaming DataFrame to table.

        Args:
            stream_df: Streaming DataFrame
            trigger: Trigger configuration
            output_mode: Output mode (append, complete, update)
            checkpoint_location: Custom checkpoint location
            options: Additional write options

        Returns:
            StreamingQuery object

        Example:
            >>> stream_df = spark.readStream.format("kafka")...
            >>> query = table.write_stream(
            ...     stream_df,
            ...     trigger=TriggerConfig.processing_time("5 seconds"),
            ...     output_mode="append"
            ... )
        """
        checkpoint_loc = checkpoint_location or f"{self.config.checkpoint_path}/{self.config.name}_stream"

        writer = stream_df.writeStream \
            .format("delta") \
            .outputMode(output_mode) \
            .option("checkpointLocation", checkpoint_loc)

        # Apply trigger
        if trigger:
            trigger_dict = trigger.to_dict()
            for key, value in trigger_dict.items():
                writer = writer.trigger(**{key: value})

        # Apply additional options
        if options:
            for key, value in options.items():
                writer = writer.option(key, value)

        # Apply table properties
        if self.config.merge_schema:
            writer = writer.option("mergeSchema", "true")

        return writer.start(self.config.path)

    def read_stream(
        self,
        options: Optional[Dict[str, str]] = None,
        max_files_per_trigger: Optional[int] = None,
        max_bytes_per_trigger: Optional[str] = None
    ) -> DataFrame:
        """Read table as a streaming DataFrame.

        Args:
            options: Additional read options
            max_files_per_trigger: Limit files per micro-batch
            max_bytes_per_trigger: Limit bytes per micro-batch (e.g., "1g")

        Returns:
            Streaming DataFrame

        Example:
            >>> stream_df = table.read_stream(max_files_per_trigger=100)
        """
        reader = self.spark.readStream.format("delta")

        if max_files_per_trigger:
            reader = reader.option("maxFilesPerTrigger", max_files_per_trigger)

        if max_bytes_per_trigger:
            reader = reader.option("maxBytesPerTrigger", max_bytes_per_trigger)

        if options:
            for key, value in options.items():
                reader = reader.option(key, value)

        return reader.load(self.config.path)

    def read_change_feed_stream(
        self,
        starting_version: Optional[int] = None,
        starting_timestamp: Optional[str] = None,
        options: Optional[Dict[str, str]] = None
    ) -> DataFrame:
        """Read change data feed as a stream.

        Args:
            starting_version: Starting version for change feed
            starting_timestamp: Starting timestamp
            options: Additional read options

        Returns:
            Streaming DataFrame with change feed

        Example:
            >>> changes = table.read_change_feed_stream(starting_version=10)
        """
        reader = self.spark.readStream.format("delta") \
            .option("readChangeFeed", "true")

        if starting_version is not None:
            reader = reader.option("startingVersion", starting_version)
        elif starting_timestamp is not None:
            reader = reader.option("startingTimestamp", starting_timestamp)

        if options:
            for key, value in options.items():
                reader = reader.option(key, value)

        return reader.load(self.config.path)

    def foreachBatch_write(
        self,
        stream_df: DataFrame,
        batch_function,
        trigger: Optional[TriggerConfig] = None,
        checkpoint_location: Optional[str] = None
    ) -> StreamingQuery:
        """Write stream using foreachBatch for custom logic.

        Args:
            stream_df: Streaming DataFrame
            batch_function: Function(batch_df, batch_id) to process each batch
            trigger: Trigger configuration
            checkpoint_location: Custom checkpoint location

        Returns:
            StreamingQuery object

        Example:
            >>> def process_batch(batch_df, batch_id):
            ...     # Custom processing
            ...     batch_df.write.format("delta").mode("append").save(table.config.path)
            >>>
            >>> query = table.foreachBatch_write(stream_df, process_batch)
        """
        checkpoint_loc = checkpoint_location or f"{self.config.checkpoint_path}/{self.config.name}_foreach"

        writer = stream_df.writeStream \
            .foreachBatch(batch_function) \
            .option("checkpointLocation", checkpoint_loc)

        if trigger:
            trigger_dict = trigger.to_dict()
            for key, value in trigger_dict.items():
                writer = writer.trigger(**{key: value})

        return writer.start()
