"""Custom exceptions for Delta Platform."""

from typing import List, Optional, Dict, Any


class DeltaPlatformError(Exception):
    """Base exception for all Delta Platform errors."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        """Initialize exception.

        Args:
            message: Error message
            details: Additional error details
        """
        super().__init__(message)
        self.message = message
        self.details = details or {}


class TableNotFoundError(DeltaPlatformError):
    """Table does not exist."""

    def __init__(self, table_name: str, path: str):
        """Initialize exception.

        Args:
            table_name: Name of the table
            path: Path where table was expected
        """
        super().__init__(
            f"Table '{table_name}' not found at path '{path}'",
            details={"table_name": table_name, "path": path}
        )
        self.table_name = table_name
        self.path = path


class ValidationError(DeltaPlatformError):
    """Data validation failed."""

    def __init__(self, failures: List):
        """Initialize exception.

        Args:
            failures: List of ValidationFailure objects
        """
        error_messages = [f.message for f in failures]
        super().__init__(
            f"Validation failed with {len(failures)} error(s):\n" + "\n".join(f"  - {msg}" for msg in error_messages),
            details={"failures": failures, "count": len(failures)}
        )
        self.failures = failures


class MergeConflictError(DeltaPlatformError):
    """Merge operation encountered conflicts."""

    def __init__(self, message: str, conflicts: Optional[Dict[str, Any]] = None):
        """Initialize exception.

        Args:
            message: Error message
            conflicts: Conflict details
        """
        super().__init__(message, details={"conflicts": conflicts})
        self.conflicts = conflicts


class SchemaEvolutionError(DeltaPlatformError):
    """Schema evolution not allowed or failed."""

    def __init__(self, message: str, expected_schema: Optional[str] = None, actual_schema: Optional[str] = None):
        """Initialize exception.

        Args:
            message: Error message
            expected_schema: Expected schema
            actual_schema: Actual schema
        """
        super().__init__(
            message,
            details={
                "expected_schema": expected_schema,
                "actual_schema": actual_schema
            }
        )
        self.expected_schema = expected_schema
        self.actual_schema = actual_schema


class OptimizationError(DeltaPlatformError):
    """Table optimization failed."""

    pass


class DataSourceError(DeltaPlatformError):
    """Error reading from data source."""

    def __init__(self, source_path: str, message: str):
        """Initialize exception.

        Args:
            source_path: Path to data source
            message: Error message
        """
        super().__init__(
            f"Error reading from '{source_path}': {message}",
            details={"source_path": source_path}
        )
        self.source_path = source_path


class TransformationError(DeltaPlatformError):
    """Error during data transformation."""

    def __init__(self, strategy_name: str, message: str):
        """Initialize exception.

        Args:
            strategy_name: Name of transformation strategy
            message: Error message
        """
        super().__init__(
            f"Transformation '{strategy_name}' failed: {message}",
            details={"strategy": strategy_name}
        )
        self.strategy_name = strategy_name


class ConfigurationError(DeltaPlatformError):
    """Invalid configuration."""

    pass


class StreamingError(DeltaPlatformError):
    """Error in streaming operations."""

    pass


class PermissionError(DeltaPlatformError):
    """Insufficient permissions for operation."""

    def __init__(self, operation: str, resource: str):
        """Initialize exception.

        Args:
            operation: Operation that was attempted
            resource: Resource that requires permissions
        """
        super().__init__(
            f"Insufficient permissions for {operation} on {resource}",
            details={"operation": operation, "resource": resource}
        )
        self.operation = operation
        self.resource = resource
