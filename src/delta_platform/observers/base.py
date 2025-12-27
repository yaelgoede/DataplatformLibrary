"""Base observer interface."""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
from pyspark.sql import DataFrame


class TableObserver(ABC):
    """
    Abstract base class for table operation observers.

    Implements the Observer pattern to monitor table operations.
    """

    @abstractmethod
    def on_write(
        self,
        table,  # DeltaTable instance
        df: DataFrame,
        mode: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Called before/after write operations.

        Args:
            table: DeltaTable instance
            df: DataFrame being written
            mode: Write mode (append, overwrite)
            metadata: Additional metadata about the operation
        """
        pass

    @abstractmethod
    def on_merge(
        self,
        table,  # DeltaTable instance
        source_df: DataFrame,
        rows_inserted: int,
        rows_updated: int,
        rows_deleted: int,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Called after merge operations.

        Args:
            table: DeltaTable instance
            source_df: Source DataFrame
            rows_inserted: Number of rows inserted
            rows_updated: Number of rows updated
            rows_deleted: Number of rows deleted
            metadata: Additional metadata
        """
        pass

    @abstractmethod
    def on_optimize(
        self,
        table,  # DeltaTable instance
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Called after optimize operations.

        Args:
            table: DeltaTable instance
            metadata: Additional metadata
        """
        pass

    @abstractmethod
    def on_vacuum(
        self,
        table,  # DeltaTable instance
        retention_hours: int,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Called after vacuum operations.

        Args:
            table: DeltaTable instance
            retention_hours: Retention period used
            metadata: Additional metadata
        """
        pass

    @abstractmethod
    def on_error(
        self,
        table,  # DeltaTable instance
        operation: str,
        error: Exception,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Called when an error occurs.

        Args:
            table: DeltaTable instance
            operation: Operation that failed
            error: Exception that occurred
            metadata: Additional metadata
        """
        pass
