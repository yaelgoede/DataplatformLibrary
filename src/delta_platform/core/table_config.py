"""Configuration for Delta tables."""

from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
from enum import Enum


class MedallionLayer(Enum):
    """Medallion architecture layers."""
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


@dataclass
class TableConfig:
    """Configuration for a Delta table.

    This class holds all configuration needed to manage a Delta table
    within the medallion architecture.
    """

    name: str
    path: str
    layer: MedallionLayer

    # Partitioning
    partition_columns: List[str] = field(default_factory=list)

    # Schema management
    merge_schema: bool = True
    overwrite_schema: bool = False

    # Optimization
    optimize_write: bool = True
    auto_compact: bool = True
    z_order_columns: List[str] = field(default_factory=list)

    # Checkpointing for streaming
    checkpoint_path: Optional[str] = None

    # Table properties
    table_properties: Dict[str, str] = field(default_factory=dict)

    # Metadata
    description: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)

    def __post_init__(self):
        """Post-initialization processing."""
        if isinstance(self.layer, str):
            self.layer = MedallionLayer(self.layer)

        if self.checkpoint_path is None:
            self.checkpoint_path = f"{self.path}/_checkpoints"

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            "name": self.name,
            "path": self.path,
            "layer": self.layer.value,
            "partition_columns": self.partition_columns,
            "merge_schema": self.merge_schema,
            "overwrite_schema": self.overwrite_schema,
            "optimize_write": self.optimize_write,
            "auto_compact": self.auto_compact,
            "z_order_columns": self.z_order_columns,
            "checkpoint_path": self.checkpoint_path,
            "table_properties": self.table_properties,
            "description": self.description,
            "tags": self.tags
        }
