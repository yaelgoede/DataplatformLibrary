"""Change Data Capture (CDC) support."""

from delta_platform.cdc.strategies import CDCStrategy, CDCOperation
from delta_platform.cdc.processor import CDCProcessor

__all__ = ["CDCStrategy", "CDCOperation", "CDCProcessor"]
