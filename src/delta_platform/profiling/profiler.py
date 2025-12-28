"""Data profiling for quality analysis."""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, countDistinct, min as _min, max as _max, avg, stddev
from pyspark.sql.types import NumericType, StringType, DateType, TimestampType
import json


@dataclass
class ColumnProfile:
    """Profile for a single column."""

    name: str
    data_type: str
    null_count: int
    null_percentage: float
    distinct_count: int
    distinct_percentage: float

    # Numeric stats
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    mean: Optional[float] = None
    stddev: Optional[float] = None

    # String stats
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    avg_length: Optional[float] = None

    # Top values
    top_values: List[tuple] = field(default_factory=list)


@dataclass
class ProfileReport:
    """Comprehensive data profile report."""

    table_name: str
    row_count: int
    column_count: int
    columns: List[ColumnProfile]
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert report to dictionary."""
        return {
            "table_name": self.table_name,
            "row_count": self.row_count,
            "column_count": self.column_count,
            "columns": [
                {
                    "name": c.name,
                    "data_type": c.data_type,
                    "null_count": c.null_count,
                    "null_percentage": c.null_percentage,
                    "distinct_count": c.distinct_count,
                    "distinct_percentage": c.distinct_percentage,
                    "min_value": c.min_value,
                    "max_value": c.max_value,
                    "mean": c.mean,
                    "stddev": c.stddev,
                    "top_values": c.top_values
                }
                for c in self.columns
            ],
            "metadata": self.metadata
        }

    def to_json(self, pretty: bool = True) -> str:
        """Convert report to JSON string."""
        indent = 2 if pretty else None
        return json.dumps(self.to_dict(), indent=indent, default=str)

    def to_html(self, output_path: Optional[str] = None) -> str:
        """Generate HTML report."""
        html = f"""
        <html>
        <head>
            <title>Data Profile: {self.table_name}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                h1 {{ color: #333; }}
                table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #4CAF50; color: white; }}
                tr:nth-child(even) {{ background-color: #f2f2f2; }}
                .summary {{ background-color: #e7f3fe; padding: 15px; border-radius: 5px; }}
            </style>
        </head>
        <body>
            <h1>Data Profile Report: {self.table_name}</h1>
            <div class="summary">
                <p><strong>Total Rows:</strong> {self.row_count:,}</p>
                <p><strong>Total Columns:</strong> {self.column_count}</p>
            </div>
            <h2>Column Profiles</h2>
            <table>
                <tr>
                    <th>Column</th>
                    <th>Type</th>
                    <th>Null %</th>
                    <th>Distinct</th>
                    <th>Min</th>
                    <th>Max</th>
                    <th>Mean</th>
                </tr>
        """

        for col_profile in self.columns:
            html += f"""
                <tr>
                    <td>{col_profile.name}</td>
                    <td>{col_profile.data_type}</td>
                    <td>{col_profile.null_percentage:.2f}%</td>
                    <td>{col_profile.distinct_count:,}</td>
                    <td>{col_profile.min_value or 'N/A'}</td>
                    <td>{col_profile.max_value or 'N/A'}</td>
                    <td>{col_profile.mean:.2f if col_profile.mean else 'N/A'}</td>
                </tr>
            """

        html += """
            </table>
        </body>
        </html>
        """

        if output_path:
            with open(output_path, 'w') as f:
                f.write(html)

        return html


class DataProfiler:
    """
    Profile data quality and statistics.

    Analyzes DataFrames to generate comprehensive quality reports.
    """

    def __init__(
        self,
        sample_top_values: int = 10,
        sample_size: Optional[int] = None
    ):
        """Initialize data profiler.

        Args:
            sample_top_values: Number of top values to sample per column
            sample_size: Sample size for profiling (None = full data)
        """
        self.sample_top_values = sample_top_values
        self.sample_size = sample_size

    def profile(self, df: DataFrame, table_name: str = "unknown") -> ProfileReport:
        """Generate comprehensive data profile.

        Args:
            df: DataFrame to profile
            table_name: Name of the table

        Returns:
            ProfileReport with statistics

        Example:
            >>> profiler = DataProfiler()
            >>> report = profiler.profile(df, "users")
            >>> report.to_html("profile.html")
        """
        # Sample if needed
        if self.sample_size and self.sample_size < df.count():
            df = df.sample(fraction=self.sample_size / df.count(), seed=42)

        row_count = df.count()
        column_count = len(df.columns)

        column_profiles = []

        for field in df.schema.fields:
            col_name = field.name
            col_type = str(field.dataType)

            # Basic stats
            null_count = df.filter(col(col_name).isNull()).count()
            null_percentage = (null_count / row_count * 100) if row_count > 0 else 0

            distinct_count = df.select(col_name).distinct().count()
            distinct_percentage = (distinct_count / row_count * 100) if row_count > 0 else 0

            profile = ColumnProfile(
                name=col_name,
                data_type=col_type,
                null_count=null_count,
                null_percentage=null_percentage,
                distinct_count=distinct_count,
                distinct_percentage=distinct_percentage
            )

            # Numeric stats
            if isinstance(field.dataType, NumericType):
                stats = df.select(
                    _min(col_name).alias("min"),
                    _max(col_name).alias("max"),
                    avg(col_name).alias("mean"),
                    stddev(col_name).alias("stddev")
                ).first()

                profile.min_value = stats["min"]
                profile.max_value = stats["max"]
                profile.mean = float(stats["mean"]) if stats["mean"] is not None else None
                profile.stddev = float(stats["stddev"]) if stats["stddev"] is not None else None

            # String stats
            if isinstance(field.dataType, StringType):
                from pyspark.sql.functions import length

                length_stats = df.select(
                    _min(length(col_name)).alias("min_len"),
                    _max(length(col_name)).alias("max_len"),
                    avg(length(col_name)).alias("avg_len")
                ).first()

                profile.min_length = length_stats["min_len"]
                profile.max_length = length_stats["max_len"]
                profile.avg_length = float(length_stats["avg_len"]) if length_stats["avg_len"] else None

            # Top values
            top_values = df.groupBy(col_name) \
                .count() \
                .orderBy(col("count").desc()) \
                .limit(self.sample_top_values) \
                .collect()

            profile.top_values = [(row[col_name], row["count"]) for row in top_values]

            column_profiles.append(profile)

        return ProfileReport(
            table_name=table_name,
            row_count=row_count,
            column_count=column_count,
            columns=column_profiles
        )

    def profile_table(self, table) -> ProfileReport:
        """Profile a DeltaTable.

        Args:
            table: DeltaTable instance

        Returns:
            ProfileReport

        Example:
            >>> profiler = DataProfiler()
            >>> report = profiler.profile_table(users_table)
        """
        df = table.read()
        return self.profile(df, table.config.name)
