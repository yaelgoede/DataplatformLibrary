"""Optimization advisor for Delta tables."""

from enum import Enum
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from pyspark.sql import DataFrame


class RecommendationType(Enum):
    """Types of optimization recommendations."""

    OPTIMIZE = "OPTIMIZE"
    VACUUM = "VACUUM"
    REPARTITION = "REPARTITION"
    Z_ORDER = "Z_ORDER"
    PARTITION_STRATEGY = "PARTITION_STRATEGY"
    SCHEMA_EVOLUTION = "SCHEMA_EVOLUTION"
    SMALL_FILES = "SMALL_FILES"
    DATA_SKEW = "DATA_SKEW"


@dataclass
class Recommendation:
    """Optimization recommendation."""

    type: RecommendationType
    message: str
    severity: str  # HIGH, MEDIUM, LOW
    details: Dict[str, Any]
    action: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "type": self.type.value,
            "message": self.message,
            "severity": self.severity,
            "details": self.details,
            "action": self.action
        }


class OptimizationAdvisor:
    """
    Analyzes Delta tables and suggests optimizations.

    Provides actionable recommendations to improve query performance
    and reduce storage costs.
    """

    def __init__(
        self,
        small_file_threshold_mb: int = 128,
        large_file_threshold_mb: int = 1024,
        high_null_percentage: float = 50.0,
        partition_skew_threshold: float = 2.0
    ):
        """Initialize optimization advisor.

        Args:
            small_file_threshold_mb: Threshold for small files in MB
            large_file_threshold_mb: Threshold for large files in MB
            high_null_percentage: Threshold for high null percentage
            partition_skew_threshold: Threshold for partition skew ratio
        """
        self.small_file_threshold = small_file_threshold_mb * 1024 * 1024  # Convert to bytes
        self.large_file_threshold = large_file_threshold_mb * 1024 * 1024
        self.high_null_percentage = high_null_percentage
        self.partition_skew_threshold = partition_skew_threshold

    def analyze(self, table) -> List[Recommendation]:
        """Analyze table and provide recommendations.

        Args:
            table: DeltaTable instance

        Returns:
            List of recommendations

        Example:
            >>> advisor = OptimizationAdvisor()
            >>> recommendations = advisor.analyze(users_table)
            >>> for rec in recommendations:
            ...     print(f"{rec.severity}: {rec.message}")
        """
        recommendations = []

        if not table.exists:
            return recommendations

        # Get table details
        details = table.get_details()

        # Check for small files
        small_files_rec = self._check_small_files(table, details)
        if small_files_rec:
            recommendations.append(small_files_rec)

        # Check if vacuum is needed
        vacuum_rec = self._check_vacuum_needed(table)
        if vacuum_rec:
            recommendations.append(vacuum_rec)

        # Check partition strategy
        partition_rec = self._check_partition_strategy(table, details)
        if partition_rec:
            recommendations.append(partition_rec)

        # Check for data skew
        skew_rec = self._check_data_skew(table)
        if skew_rec:
            recommendations.append(skew_rec)

        # Check if Z-ordering would help
        z_order_rec = self._check_z_order_opportunity(table)
        if z_order_rec:
            recommendations.append(z_order_rec)

        return recommendations

    def _check_small_files(self, table, details: Dict[str, Any]) -> Optional[Recommendation]:
        """Check for small file problem."""
        num_files = details.get("num_files", 0)
        size_bytes = details.get("size_bytes", 0)

        if num_files == 0:
            return None

        avg_file_size = size_bytes / num_files

        if avg_file_size < self.small_file_threshold:
            return Recommendation(
                type=RecommendationType.SMALL_FILES,
                message=f"Table has many small files (avg: {avg_file_size / (1024*1024):.2f} MB)",
                severity="HIGH",
                details={
                    "num_files": num_files,
                    "avg_file_size_mb": avg_file_size / (1024 * 1024),
                    "total_size_mb": size_bytes / (1024 * 1024)
                },
                action=f"Run table.optimize() to compact small files"
            )

        return None

    def _check_vacuum_needed(self, table) -> Optional[Recommendation]:
        """Check if vacuum is recommended."""
        try:
            # Check if there are old versions
            history = table.history(limit=100)
            version_count = history.count()

            if version_count > 50:
                return Recommendation(
                    type=RecommendationType.VACUUM,
                    message=f"Table has {version_count}+ versions, vacuum recommended",
                    severity="MEDIUM",
                    details={"version_count": version_count},
                    action="Run table.vacuum() to remove old files"
                )
        except:
            pass

        return None

    def _check_partition_strategy(self, table, details: Dict[str, Any]) -> Optional[Recommendation]:
        """Check partition strategy."""
        partition_columns = details.get("partition_columns", [])

        if not partition_columns:
            # Check if table would benefit from partitioning
            row_count = table.count()

            if row_count > 1_000_000:  # 1M rows
                return Recommendation(
                    type=RecommendationType.PARTITION_STRATEGY,
                    message="Large table without partitioning detected",
                    severity="MEDIUM",
                    details={"row_count": row_count},
                    action="Consider partitioning by date or other high-cardinality column"
                )

        return None

    def _check_data_skew(self, table) -> Optional[Recommendation]:
        """Check for data skew in partitions."""
        if not table.config.partition_columns:
            return None

        try:
            df = table.read()

            # Get partition distribution
            partition_counts = df.groupBy(*table.config.partition_columns).count()
            stats = partition_counts.agg(
                {"count": "min", "count": "max", "count": "avg"}
            ).first()

            min_count = stats["min(count)"]
            max_count = stats["max(count)"]
            avg_count = stats["avg(count)"]

            if max_count > avg_count * self.partition_skew_threshold:
                return Recommendation(
                    type=RecommendationType.DATA_SKEW,
                    message=f"Partition skew detected (max/avg ratio: {max_count/avg_count:.2f})",
                    severity="HIGH",
                    details={
                        "min_partition_size": min_count,
                        "max_partition_size": max_count,
                        "avg_partition_size": avg_count,
                        "skew_ratio": max_count / avg_count
                    },
                    action="Consider repartitioning or adjusting partition strategy"
                )
        except:
            pass

        return None

    def _check_z_order_opportunity(self, table) -> Optional[Recommendation]:
        """Check if Z-ordering would help."""
        if not table.config.z_order_columns:
            # Suggest Z-ordering based on frequently filtered columns
            return Recommendation(
                type=RecommendationType.Z_ORDER,
                message="No Z-ordering configured",
                severity="LOW",
                details={},
                action="Consider Z-ordering on frequently filtered columns"
            )

        return None

    def generate_report(self, table) -> str:
        """Generate text report with recommendations.

        Args:
            table: DeltaTable instance

        Returns:
            Formatted text report

        Example:
            >>> report = advisor.generate_report(users_table)
            >>> print(report)
        """
        recommendations = self.analyze(table)

        report = f"Optimization Report for {table.config.name}\n"
        report += "=" * 60 + "\n\n"

        if not recommendations:
            report += "✓ No optimization issues detected.\n"
            return report

        # Group by severity
        high = [r for r in recommendations if r.severity == "HIGH"]
        medium = [r for r in recommendations if r.severity == "MEDIUM"]
        low = [r for r in recommendations if r.severity == "LOW"]

        if high:
            report += "🔴 HIGH PRIORITY:\n"
            for rec in high:
                report += f"  - {rec.message}\n"
                if rec.action:
                    report += f"    Action: {rec.action}\n"
            report += "\n"

        if medium:
            report += "🟡 MEDIUM PRIORITY:\n"
            for rec in medium:
                report += f"  - {rec.message}\n"
                if rec.action:
                    report += f"    Action: {rec.action}\n"
            report += "\n"

        if low:
            report += "🟢 LOW PRIORITY:\n"
            for rec in low:
                report += f"  - {rec.message}\n"
                if rec.action:
                    report += f"    Action: {rec.action}\n"

        return report
