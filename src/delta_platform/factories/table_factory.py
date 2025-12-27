"""Factory for creating Delta tables from various sources."""

from typing import Dict, Any, Optional
import yaml
from pyspark.sql import SparkSession

from delta_platform.core.table import DeltaTable
from delta_platform.core.table_builder import TableBuilder
from delta_platform.core.table_config import TableConfig, MedallionLayer
from delta_platform.exceptions import ConfigurationError


class TableFactory:
    """
    Factory for creating DeltaTable instances from various sources.

    Implements the Factory pattern to centralize table creation logic.
    """

    @staticmethod
    def from_yaml(spark: SparkSession, yaml_path: str) -> DeltaTable:
        """Create table from YAML configuration file.

        Args:
            spark: Active SparkSession
            yaml_path: Path to YAML configuration file

        Returns:
            Configured DeltaTable instance

        Example:
            >>> table = TableFactory.from_yaml(spark, "config/users_table.yaml")

        YAML Format:
            name: users
            path: /data/bronze/users
            layer: bronze
            partition_columns:
              - country
              - region
            z_order_columns:
              - user_id
            description: User data from CRM system
            tags:
              owner: data-engineering
              pii: "true"
        """
        try:
            with open(yaml_path, 'r') as f:
                config_dict = yaml.safe_load(f)
        except Exception as e:
            raise ConfigurationError(f"Failed to load YAML from {yaml_path}: {e}")

        return TableFactory.from_dict(spark, config_dict)

    @staticmethod
    def from_dict(spark: SparkSession, config: Dict[str, Any]) -> DeltaTable:
        """Create table from dictionary configuration.

        Args:
            spark: Active SparkSession
            config: Configuration dictionary

        Returns:
            Configured DeltaTable instance

        Example:
            >>> config = {
            ...     "name": "users",
            ...     "path": "/data/bronze/users",
            ...     "layer": "bronze",
            ...     "partition_columns": ["country"]
            ... }
            >>> table = TableFactory.from_dict(spark, config)
        """
        # Required fields
        required_fields = ["name", "path", "layer"]
        missing = [f for f in required_fields if f not in config]

        if missing:
            raise ConfigurationError(f"Missing required fields: {missing}")

        # Parse layer
        layer_value = config["layer"]
        if isinstance(layer_value, str):
            try:
                layer = MedallionLayer(layer_value.lower())
            except ValueError:
                raise ConfigurationError(f"Invalid layer: {layer_value}. Must be bronze, silver, or gold")
        else:
            layer = layer_value

        # Build table
        builder = TableBuilder(spark) \
            .name(config["name"]) \
            .path(config["path"]) \
            .layer(layer)

        # Optional fields
        if "partition_columns" in config:
            builder = builder.partition_by(config["partition_columns"])

        if "z_order_columns" in config:
            builder = builder.z_order_by(config["z_order_columns"])

        if "merge_schema" in config:
            builder = builder.merge_schema(config["merge_schema"])

        if "overwrite_schema" in config:
            builder = builder.overwrite_schema(config["overwrite_schema"])

        if "optimize_write" in config:
            builder = builder.optimize_write(config["optimize_write"])

        if "auto_compact" in config:
            builder = builder.auto_compact(config["auto_compact"])

        if "checkpoint_path" in config:
            builder = builder.checkpoint_path(config["checkpoint_path"])

        if "description" in config:
            builder = builder.description(config["description"])

        if "tags" in config:
            for key, value in config["tags"].items():
                builder = builder.tag(key, str(value))

        if "table_properties" in config:
            for key, value in config["table_properties"].items():
                builder = builder.table_property(key, str(value))

        return builder.build()

    @staticmethod
    def from_config(spark: SparkSession, config: TableConfig) -> DeltaTable:
        """Create table from TableConfig object.

        Args:
            spark: Active SparkSession
            config: TableConfig instance

        Returns:
            DeltaTable instance

        Example:
            >>> config = TableConfig(name="users", path="/data/bronze/users", layer=MedallionLayer.BRONZE)
            >>> table = TableFactory.from_config(spark, config)
        """
        return DeltaTable(spark, config)

    @staticmethod
    def from_unity_catalog(
        spark: SparkSession,
        catalog: str,
        schema: str,
        table: str,
        layer: Optional[MedallionLayer] = None
    ) -> DeltaTable:
        """Create table from Unity Catalog reference.

        Args:
            spark: Active SparkSession
            catalog: Catalog name
            schema: Schema name
            table: Table name
            layer: Medallion layer (inferred from schema name if not provided)

        Returns:
            DeltaTable instance

        Example:
            >>> table = TableFactory.from_unity_catalog(
            ...     spark,
            ...     catalog="main",
            ...     schema="bronze",
            ...     table="users"
            ... )
        """
        # Infer layer from schema name if not provided
        if layer is None:
            schema_lower = schema.lower()
            if "bronze" in schema_lower:
                layer = MedallionLayer.BRONZE
            elif "silver" in schema_lower:
                layer = MedallionLayer.SILVER
            elif "gold" in schema_lower:
                layer = MedallionLayer.GOLD
            else:
                layer = MedallionLayer.BRONZE  # Default

        # Get table location from Unity Catalog
        try:
            table_info = spark.sql(f"DESCRIBE DETAIL {catalog}.{schema}.{table}").collect()[0]
            path = table_info.location
        except Exception as e:
            raise ConfigurationError(f"Failed to get table from Unity Catalog: {e}")

        return (TableBuilder(spark)
                .name(table)
                .path(path)
                .layer(layer)
                .description(f"Unity Catalog table: {catalog}.{schema}.{table}")
                .tag("catalog", catalog)
                .tag("schema", schema)
                .build())

    @staticmethod
    def create_multiple(spark: SparkSession, configs: Dict[str, Dict[str, Any]]) -> Dict[str, DeltaTable]:
        """Create multiple tables from configuration dictionary.

        Args:
            spark: Active SparkSession
            configs: Dictionary of {table_name: config_dict}

        Returns:
            Dictionary of {table_name: DeltaTable}

        Example:
            >>> configs = {
            ...     "users": {"name": "users", "path": "/data/bronze/users", "layer": "bronze"},
            ...     "orders": {"name": "orders", "path": "/data/bronze/orders", "layer": "bronze"}
            ... }
            >>> tables = TableFactory.create_multiple(spark, configs)
            >>> users_table = tables["users"]
        """
        tables = {}

        for name, config in configs.items():
            try:
                tables[name] = TableFactory.from_dict(spark, config)
            except Exception as e:
                raise ConfigurationError(f"Failed to create table '{name}': {e}")

        return tables
