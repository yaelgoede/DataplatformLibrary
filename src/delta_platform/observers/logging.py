"""Logging observer for table operations."""

import logging
from typing import Dict, Any, Optional
from pyspark.sql import DataFrame

from delta_platform.observers.base import TableObserver


class LoggingObserver(TableObserver):
    """
    Observer that logs table operations.

    Uses Python's logging module to record operations.
    """

    def __init__(self, logger: Optional[logging.Logger] = None, level: int = logging.INFO):
        """Initialize logging observer.

        Args:
            logger: Logger instance (creates one if not provided)
            level: Logging level
        """
        self.logger = logger or logging.getLogger("delta_platform")
        self.level = level

    def on_write(
        self,
        table,
        df: DataFrame,
        mode: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Log write operations."""
        try:
            row_count = df.count()
            self.logger.log(
                self.level,
                f"Write to {table.config.name}: mode={mode}, rows={row_count}, layer={table.config.layer.value}"
            )
        except Exception as e:
            self.logger.warning(f"Could not log write operation: {e}")

    def on_merge(
        self,
        table,
        source_df: DataFrame,
        rows_inserted: int,
        rows_updated: int,
        rows_deleted: int,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Log merge operations."""
        self.logger.log(
            self.level,
            f"Merge into {table.config.name}: inserted={rows_inserted}, updated={rows_updated}, deleted={rows_deleted}"
        )

    def on_optimize(
        self,
        table,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Log optimize operations."""
        self.logger.log(
            self.level,
            f"Optimized {table.config.name} at {table.config.path}"
        )

    def on_vacuum(
        self,
        table,
        retention_hours: int,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Log vacuum operations."""
        self.logger.log(
            self.level,
            f"Vacuumed {table.config.name}: retention_hours={retention_hours}"
        )

    def on_error(
        self,
        table,
        operation: str,
        error: Exception,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Log errors."""
        self.logger.error(
            f"Error in {operation} on {table.config.name}: {type(error).__name__}: {error}"
        )
