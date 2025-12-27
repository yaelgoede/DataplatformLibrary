"""
Delta Platform Library

A Python library for managing Delta Lake data platforms with medallion architecture on Databricks Spark.
"""

from delta_platform.platform import DeltaPlatform
from delta_platform.config import PlatformConfig
from delta_platform.layers.bronze import BronzeLayer
from delta_platform.layers.silver import SilverLayer
from delta_platform.layers.gold import GoldLayer

__version__ = "0.1.0"
__all__ = [
    "DeltaPlatform",
    "PlatformConfig",
    "BronzeLayer",
    "SilverLayer",
    "GoldLayer",
]
