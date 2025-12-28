"""Trigger configuration for streaming."""

from enum import Enum
from dataclasses import dataclass
from typing import Optional


class TriggerType(Enum):
    """Types of streaming triggers."""

    PROCESSING_TIME = "processingTime"
    ONCE = "once"
    CONTINUOUS = "continuous"
    AVAILABLE_NOW = "availableNow"


@dataclass
class TriggerConfig:
    """Configuration for streaming triggers."""

    trigger_type: TriggerType
    interval: Optional[str] = None  # For PROCESSING_TIME and CONTINUOUS

    def to_dict(self):
        """Convert to dictionary for Spark API."""
        if self.trigger_type == TriggerType.ONCE:
            return {"once": True}
        elif self.trigger_type == TriggerType.AVAILABLE_NOW:
            return {"availableNow": True}
        elif self.trigger_type == TriggerType.CONTINUOUS:
            return {"continuous": self.interval or "1 second"}
        else:  # PROCESSING_TIME
            return {"processingTime": self.interval or "10 seconds"}

    @classmethod
    def processing_time(cls, interval: str = "10 seconds"):
        """Create processing time trigger."""
        return cls(TriggerType.PROCESSING_TIME, interval)

    @classmethod
    def once(cls):
        """Create one-time trigger."""
        return cls(TriggerType.ONCE)

    @classmethod
    def available_now(cls):
        """Create available now trigger."""
        return cls(TriggerType.AVAILABLE_NOW)

    @classmethod
    def continuous(cls, interval: str = "1 second"):
        """Create continuous trigger."""
        return cls(TriggerType.CONTINUOUS, interval)
