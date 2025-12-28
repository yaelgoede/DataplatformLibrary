"""Benchmarking tools for table operations."""

import time
from typing import List, Dict, Any, Callable
from dataclasses import dataclass, field
from pyspark.sql import DataFrame


@dataclass
class BenchmarkResult:
    """Result of a benchmark run."""

    operation: str
    duration_seconds: float
    rows_processed: int
    throughput_rows_per_sec: float
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __repr__(self):
        return f"BenchmarkResult({self.operation}: {self.duration_seconds:.2f}s, {self.throughput_rows_per_sec:.0f} rows/s)"


class PerformanceBenchmark:
    """
    Benchmark table operations for performance analysis.

    Measures execution time, throughput, and resource usage.
    """

    def __init__(self):
        """Initialize performance benchmark."""
        self.results: List[BenchmarkResult] = []

    def benchmark_write(
        self,
        table,
        df: DataFrame,
        mode: str = "append"
    ) -> BenchmarkResult:
        """Benchmark write performance.

        Args:
            table: DeltaTable instance
            df: DataFrame to write
            mode: Write mode

        Returns:
            BenchmarkResult

        Example:
            >>> benchmark = PerformanceBenchmark()
            >>> result = benchmark.benchmark_write(table, df)
            >>> print(result)
        """
        row_count = df.count()

        start_time = time.time()
        table._write(df, mode=mode)
        duration = time.time() - start_time

        throughput = row_count / duration if duration > 0 else 0

        result = BenchmarkResult(
            operation=f"write_{mode}",
            duration_seconds=duration,
            rows_processed=row_count,
            throughput_rows_per_sec=throughput,
            metadata={
                "mode": mode,
                "table": table.config.name
            }
        )

        self.results.append(result)
        return result

    def benchmark_read(self, table) -> BenchmarkResult:
        """Benchmark read performance.

        Args:
            table: DeltaTable instance

        Returns:
            BenchmarkResult
        """
        start_time = time.time()
        df = table.read()
        row_count = df.count()
        duration = time.time() - start_time

        throughput = row_count / duration if duration > 0 else 0

        result = BenchmarkResult(
            operation="read",
            duration_seconds=duration,
            rows_processed=row_count,
            throughput_rows_per_sec=throughput,
            metadata={"table": table.config.name}
        )

        self.results.append(result)
        return result

    def benchmark_transformation(
        self,
        df: DataFrame,
        transformation: Callable[[DataFrame], DataFrame],
        operation_name: str = "transformation"
    ) -> BenchmarkResult:
        """Benchmark transformation performance.

        Args:
            df: Input DataFrame
            transformation: Transformation function
            operation_name: Name of the operation

        Returns:
            BenchmarkResult
        """
        start_time = time.time()
        result_df = transformation(df)
        row_count = result_df.count()
        duration = time.time() - start_time

        throughput = row_count / duration if duration > 0 else 0

        result = BenchmarkResult(
            operation=operation_name,
            duration_seconds=duration,
            rows_processed=row_count,
            throughput_rows_per_sec=throughput
        )

        self.results.append(result)
        return result

    def compare_strategies(
        self,
        strategies: Dict[str, Any],
        df: DataFrame
    ) -> Dict[str, BenchmarkResult]:
        """Compare performance of different transformation strategies.

        Args:
            strategies: Dict of {name: strategy}
            df: Input DataFrame

        Returns:
            Dict of {name: BenchmarkResult}

        Example:
            >>> strategies = {
            ...     "strategy_a": CleaningStrategy(...),
            ...     "strategy_b": DeduplicationStrategy(...)
            ... }
            >>> results = benchmark.compare_strategies(strategies, df)
        """
        results = {}

        for name, strategy in strategies.items():
            result = self.benchmark_transformation(
                df,
                lambda d: strategy.transform(d),
                operation_name=name
            )
            results[name] = result

        return results

    def get_summary(self) -> Dict[str, Any]:
        """Get summary of all benchmark results.

        Returns:
            Summary dictionary
        """
        if not self.results:
            return {"message": "No benchmark results"}

        total_duration = sum(r.duration_seconds for r in self.results)
        total_rows = sum(r.rows_processed for r in self.results)

        return {
            "total_operations": len(self.results),
            "total_duration_seconds": total_duration,
            "total_rows_processed": total_rows,
            "average_throughput": total_rows / total_duration if total_duration > 0 else 0,
            "results": [
                {
                    "operation": r.operation,
                    "duration": r.duration_seconds,
                    "throughput": r.throughput_rows_per_sec
                }
                for r in self.results
            ]
        }

    def reset(self):
        """Clear all benchmark results."""
        self.results.clear()
