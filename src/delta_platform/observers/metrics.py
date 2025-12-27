"""Metrics collection observer."""

from typing import Dict, Any, Optional, List
from datetime import datetime
from pyspark.sql import DataFrame

from delta_platform.observers.base import TableObserver


class MetricsObserver(TableObserver):
    """
    Observer that collects metrics on table operations.

    Tracks operation counts, timing, data volumes, etc.
    """

    def __init__(self):
        """Initialize metrics observer."""
        self.metrics: List[Dict[str, Any]] = []

    def on_write(
        self,
        table,
        df: DataFrame,
        mode: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Record write operation metrics."""
        try:
            row_count = df.count()
        except:
            row_count = None

        self._record_metric({
            "operation": "write",
            "table_name": table.config.name,
            "table_path": table.config.path,
            "layer": table.config.layer.value,
            "mode": mode,
            "row_count": row_count,
            "timestamp": datetime.now().isoformat(),
            **(metadata or {})
        })

    def on_merge(
        self,
        table,
        source_df: DataFrame,
        rows_inserted: int,
        rows_updated: int,
        rows_deleted: int,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Record merge operation metrics."""
        self._record_metric({
            "operation": "merge",
            "table_name": table.config.name,
            "table_path": table.config.path,
            "layer": table.config.layer.value,
            "rows_inserted": rows_inserted,
            "rows_updated": rows_updated,
            "rows_deleted": rows_deleted,
            "total_changes": rows_inserted + rows_updated + rows_deleted,
            "timestamp": datetime.now().isoformat(),
            **(metadata or {})
        })

    def on_optimize(
        self,
        table,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Record optimize operation metrics."""
        self._record_metric({
            "operation": "optimize",
            "table_name": table.config.name,
            "table_path": table.config.path,
            "layer": table.config.layer.value,
            "timestamp": datetime.now().isoformat(),
            **(metadata or {})
        })

    def on_vacuum(
        self,
        table,
        retention_hours: int,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Record vacuum operation metrics."""
        self._record_metric({
            "operation": "vacuum",
            "table_name": table.config.name,
            "table_path": table.config.path,
            "layer": table.config.layer.value,
            "retention_hours": retention_hours,
            "timestamp": datetime.now().isoformat(),
            **(metadata or {})
        })

    def on_error(
        self,
        table,
        operation: str,
        error: Exception,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Record error metrics."""
        self._record_metric({
            "operation": operation,
            "table_name": table.config.name,
            "table_path": table.config.path,
            "layer": table.config.layer.value,
            "status": "error",
            "error_type": type(error).__name__,
            "error_message": str(error),
            "timestamp": datetime.now().isoformat(),
            **(metadata or {})
        })

    def _record_metric(self, metric: Dict[str, Any]) -> None:
        """Record a metric."""
        self.metrics.append(metric)

    def get_metrics(self) -> List[Dict[str, Any]]:
        """Get all collected metrics.

        Returns:
            List of metric dictionaries
        """
        return self.metrics.copy()

    def get_summary(self) -> Dict[str, Any]:
        """Get summary statistics of collected metrics.

        Returns:
            Summary statistics
        """
        total_operations = len(self.metrics)
        errors = [m for m in self.metrics if m.get("status") == "error"]
        operations_by_type = {}

        for metric in self.metrics:
            op = metric.get("operation", "unknown")
            operations_by_type[op] = operations_by_type.get(op, 0) + 1

        return {
            "total_operations": total_operations,
            "total_errors": len(errors),
            "operations_by_type": operations_by_type,
            "error_rate": len(errors) / total_operations if total_operations > 0 else 0
        }

    def reset(self) -> None:
        """Clear all collected metrics."""
        self.metrics.clear()
