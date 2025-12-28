"""Enhanced streaming support for Delta tables."""

from delta_platform.streaming.streaming_table import StreamingTable
from delta_platform.streaming.trigger import TriggerConfig, TriggerType

__all__ = ["StreamingTable", "TriggerConfig", "TriggerType"]
