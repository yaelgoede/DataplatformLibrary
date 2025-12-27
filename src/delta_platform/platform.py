"""Main Delta Platform class orchestrating all layers."""

from typing import Optional
from pyspark.sql import SparkSession

from delta_platform.config import PlatformConfig
from delta_platform.layers.bronze import BronzeLayer
from delta_platform.layers.silver import SilverLayer
from delta_platform.layers.gold import GoldLayer


class DeltaPlatform:
    """Main class for managing a Delta Lake data platform with medallion architecture."""

    def __init__(
        self,
        config: PlatformConfig,
        spark: Optional[SparkSession] = None
    ):
        """Initialize the Delta Platform.

        Args:
            config: Platform configuration
            spark: Optional SparkSession (creates one if not provided)
        """
        self.config = config
        self.spark = spark or self._create_spark_session()

        # Initialize layers
        self.bronze = BronzeLayer(
            self.spark,
            config.bronze.path,
            config.bronze.checkpoint_path
        )
        self.silver = SilverLayer(
            self.spark,
            config.silver.path,
            config.silver.checkpoint_path
        )
        self.gold = GoldLayer(
            self.spark,
            config.gold.path,
            config.gold.checkpoint_path
        )

    def _create_spark_session(self) -> SparkSession:
        """Create a SparkSession with Delta Lake support.

        Returns:
            Configured SparkSession
        """
        builder = SparkSession.builder \
            .appName("DeltaPlatform") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        # Apply custom spark configurations
        for key, value in self.config.spark_config.items():
            builder = builder.config(key, value)

        # Set catalog and schema if specified
        if self.config.catalog:
            builder = builder.config("spark.sql.catalog.default", self.config.catalog)

        return builder.getOrCreate()

    def optimize_all_tables(self, z_order_columns: Optional[dict] = None) -> None:
        """Optimize all tables in all layers.

        Args:
            z_order_columns: Dict of {table_path: [z_order_columns]}
        """
        z_order_columns = z_order_columns or {}

        layers = [
            ("bronze", self.bronze),
            ("silver", self.silver),
            ("gold", self.gold)
        ]

        for layer_name, layer in layers:
            # Get all tables in this layer
            try:
                tables = self.spark._jvm.org.apache.hadoop.fs.FileSystem \
                    .get(self.spark._jsc.hadoopConfiguration()) \
                    .listStatus(self.spark._jvm.org.apache.hadoop.fs.Path(layer.layer_path))

                for table_status in tables:
                    table_name = table_status.getPath().getName()
                    if not table_name.startswith("_"):
                        table_path = layer.table_path(table_name)
                        z_order = z_order_columns.get(table_path)
                        layer.optimize_table(table_name, z_order_by=z_order)
                        print(f"Optimized {layer_name}.{table_name}")
            except Exception as e:
                print(f"Could not optimize {layer_name} layer: {e}")

    def vacuum_all_tables(self, retention_hours: int = 168) -> None:
        """Vacuum all tables in all layers.

        Args:
            retention_hours: Retention period in hours (default 7 days)
        """
        layers = [
            ("bronze", self.bronze),
            ("silver", self.silver),
            ("gold", self.gold)
        ]

        for layer_name, layer in layers:
            try:
                tables = self.spark._jvm.org.apache.hadoop.fs.FileSystem \
                    .get(self.spark._jsc.hadoopConfiguration()) \
                    .listStatus(self.spark._jvm.org.apache.hadoop.fs.Path(layer.layer_path))

                for table_status in tables:
                    table_name = table_status.getPath().getName()
                    if not table_name.startswith("_"):
                        layer.vacuum_table(table_name, retention_hours)
                        print(f"Vacuumed {layer_name}.{table_name}")
            except Exception as e:
                print(f"Could not vacuum {layer_name} layer: {e}")

    def get_table_info(self, layer: str, table_name: str) -> dict:
        """Get information about a specific table.

        Args:
            layer: Layer name (bronze, silver, gold)
            table_name: Table name

        Returns:
            Dict with table information
        """
        layer_obj = getattr(self, layer.lower())
        delta_table = layer_obj.get_delta_table(table_name)

        return {
            "path": layer_obj.table_path(table_name),
            "version": delta_table.history(1).collect()[0]["version"],
            "schema": layer_obj.read_table(table_name).schema.simpleString()
        }

    def stop(self) -> None:
        """Stop the Spark session."""
        if self.spark:
            self.spark.stop()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop()
