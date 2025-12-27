"""Configuration management for Delta Platform."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional
import yaml


@dataclass
class LayerConfig:
    """Configuration for a single medallion layer."""

    path: str
    checkpoint_path: Optional[str] = None
    partition_columns: list[str] = field(default_factory=list)
    merge_schema: bool = True
    overwrite_schema: bool = False


@dataclass
class PlatformConfig:
    """Main configuration for the Delta Platform."""

    bronze: LayerConfig
    silver: LayerConfig
    gold: LayerConfig
    spark_config: Dict[str, str] = field(default_factory=dict)
    catalog: Optional[str] = None
    schema: Optional[str] = None

    @classmethod
    def from_yaml(cls, config_path: str) -> "PlatformConfig":
        """Load configuration from a YAML file.

        Args:
            config_path: Path to the YAML configuration file

        Returns:
            PlatformConfig instance
        """
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)

        bronze_config = LayerConfig(**config_data.get('bronze', {}))
        silver_config = LayerConfig(**config_data.get('silver', {}))
        gold_config = LayerConfig(**config_data.get('gold', {}))

        return cls(
            bronze=bronze_config,
            silver=silver_config,
            gold=gold_config,
            spark_config=config_data.get('spark_config', {}),
            catalog=config_data.get('catalog'),
            schema=config_data.get('schema')
        )

    @classmethod
    def create_default(cls, base_path: str) -> "PlatformConfig":
        """Create a default configuration with standard paths.

        Args:
            base_path: Base path for all layers

        Returns:
            PlatformConfig instance with default settings
        """
        return cls(
            bronze=LayerConfig(
                path=f"{base_path}/bronze",
                checkpoint_path=f"{base_path}/bronze/_checkpoints"
            ),
            silver=LayerConfig(
                path=f"{base_path}/silver",
                checkpoint_path=f"{base_path}/silver/_checkpoints"
            ),
            gold=LayerConfig(
                path=f"{base_path}/gold",
                checkpoint_path=f"{base_path}/gold/_checkpoints"
            ),
            spark_config={
                "spark.databricks.delta.optimizeWrite.enabled": "true",
                "spark.databricks.delta.autoCompact.enabled": "true"
            }
        )
