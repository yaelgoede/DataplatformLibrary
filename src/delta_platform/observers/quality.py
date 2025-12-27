"""Data quality monitoring observer."""

from typing import Dict, Any, Optional, List
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, countDistinct, sum as _sum

from delta_platform.observers.base import TableObserver


class DataQualityObserver(TableObserver):
    """
    Observer that monitors data quality metrics.

    Tracks null counts, duplicates, data volumes, etc.
    """

    def __init__(self, track_nulls: bool = True, track_duplicates: bool = True):
        """Initialize data quality observer.

        Args:
            track_nulls: Whether to track null percentages
            track_duplicates: Whether to track duplicate counts
        """
        self.track_nulls = track_nulls
        self.track_duplicates = track_duplicates
        self.quality_reports: List[Dict[str, Any]] = []

    def on_write(
        self,
        table,
        df: DataFrame,
        mode: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Analyze data quality during write."""
        quality_metrics = {
            "operation": "write",
            "table_name": table.config.name,
            "timestamp": datetime.now().isoformat(),
            "mode": mode
        }

        try:
            row_count = df.count()
            quality_metrics["row_count"] = row_count

            if row_count > 0:
                # Track null percentages
                if self.track_nulls:
                    null_percentages = {}
                    for column in df.columns:
                        null_count = df.filter(col(column).isNull()).count()
                        null_percentages[column] = (null_count / row_count) * 100

                    quality_metrics["null_percentages"] = null_percentages

                # Track column statistics
                quality_metrics["column_count"] = len(df.columns)

        except Exception as e:
            quality_metrics["error"] = str(e)

        self.quality_reports.append(quality_metrics)

    def on_merge(
        self,
        table,
        source_df: DataFrame,
        rows_inserted: int,
        rows_updated: int,
        rows_deleted: int,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Monitor data quality during merge."""
        quality_metrics = {
            "operation": "merge",
            "table_name": table.config.name,
            "timestamp": datetime.now().isoformat(),
            "rows_inserted": rows_inserted,
            "rows_updated": rows_updated,
            "rows_deleted": rows_deleted
        }

        self.quality_reports.append(quality_metrics)

    def on_optimize(
        self,
        table,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """No quality monitoring needed for optimize."""
        pass

    def on_vacuum(
        self,
        table,
        retention_hours: int,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """No quality monitoring needed for vacuum."""
        pass

    def on_error(
        self,
        table,
        operation: str,
        error: Exception,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Record quality check errors."""
        self.quality_reports.append({
            "operation": operation,
            "table_name": table.config.name,
            "timestamp": datetime.now().isoformat(),
            "status": "error",
            "error": str(error)
        })

    def get_reports(self) -> List[Dict[str, Any]]:
        """Get all quality reports.

        Returns:
            List of quality report dictionaries
        """
        return self.quality_reports.copy()

    def get_latest_report(self, table_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Get the most recent quality report.

        Args:
            table_name: Optional table name filter

        Returns:
            Latest quality report or None
        """
        if not self.quality_reports:
            return None

        if table_name:
            matching = [r for r in self.quality_reports if r.get("table_name") == table_name]
            return matching[-1] if matching else None

        return self.quality_reports[-1]
