"""Validation rules for data quality checks."""

from abc import ABC, abstractmethod
from typing import List, Optional, Callable, Dict, Any, Set
from dataclasses import dataclass, field
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField
import re


@dataclass
class ValidationFailure:
    """Represents a single validation failure."""

    rule_name: str
    message: str
    column: Optional[str] = None
    severity: str = "ERROR"  # ERROR, WARNING, INFO
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationResult:
    """Result of validation checks."""

    passed: bool
    failures: List[ValidationFailure] = field(default_factory=list)
    warnings: List[ValidationFailure] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def errors(self) -> List[ValidationFailure]:
        """Get only error-level failures."""
        return [f for f in self.failures if f.severity == "ERROR"]

    @property
    def has_errors(self) -> bool:
        """Check if there are any errors."""
        return len(self.errors) > 0

    @property
    def has_warnings(self) -> bool:
        """Check if there are any warnings."""
        return len(self.warnings) > 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary."""
        return {
            "passed": self.passed,
            "error_count": len(self.errors),
            "warning_count": len(self.warnings),
            "failures": [
                {
                    "rule": f.rule_name,
                    "message": f.message,
                    "column": f.column,
                    "severity": f.severity,
                    "details": f.details
                }
                for f in self.failures + self.warnings
            ],
            "metadata": self.metadata
        }


class ValidationRule(ABC):
    """
    Abstract base class for validation rules.

    Uses the Chain of Responsibility pattern to allow multiple
    validation rules to be applied in sequence.
    """

    def __init__(self, name: Optional[str] = None, severity: str = "ERROR"):
        """Initialize validation rule.

        Args:
            name: Name of the validation rule
            severity: Severity level (ERROR, WARNING, INFO)
        """
        self.name = name or self.__class__.__name__
        self.severity = severity

    @abstractmethod
    def validate(self, df: DataFrame) -> ValidationResult:
        """Validate the DataFrame.

        Args:
            df: DataFrame to validate

        Returns:
            ValidationResult with pass/fail status and details
        """
        pass

    def _create_failure(self, message: str, column: Optional[str] = None, **details) -> ValidationFailure:
        """Create a validation failure."""
        return ValidationFailure(
            rule_name=self.name,
            message=message,
            column=column,
            severity=self.severity,
            details=details
        )


class SchemaValidation(ValidationRule):
    """Validate DataFrame schema matches expected structure."""

    def __init__(
        self,
        expected_schema: Optional[StructType] = None,
        required_columns: Optional[List[str]] = None,
        column_types: Optional[Dict[str, str]] = None,
        allow_extra_columns: bool = True,
        name: Optional[str] = None
    ):
        """Initialize schema validation.

        Args:
            expected_schema: Complete expected schema
            required_columns: List of required column names
            column_types: Dict of column_name -> expected_type
            allow_extra_columns: Whether to allow extra columns
            name: Rule name
        """
        super().__init__(name or "SchemaValidation")
        self.expected_schema = expected_schema
        self.required_columns = required_columns or []
        self.column_types = column_types or {}
        self.allow_extra_columns = allow_extra_columns

    def validate(self, df: DataFrame) -> ValidationResult:
        """Validate schema."""
        failures = []

        actual_columns = set(df.columns)

        # Check required columns
        if self.required_columns:
            required_set = set(self.required_columns)
            missing = required_set - actual_columns

            if missing:
                failures.append(self._create_failure(
                    f"Missing required columns: {sorted(missing)}",
                    details={"missing_columns": list(missing)}
                ))

        # Check column types
        if self.column_types:
            for col_name, expected_type in self.column_types.items():
                if col_name in df.columns:
                    actual_type = dict(df.dtypes)[col_name]
                    if actual_type != expected_type:
                        failures.append(self._create_failure(
                            f"Column '{col_name}' has type '{actual_type}', expected '{expected_type}'",
                            column=col_name,
                            details={
                                "expected_type": expected_type,
                                "actual_type": actual_type
                            }
                        ))

        # Check for extra columns
        if not self.allow_extra_columns and self.required_columns:
            extra = actual_columns - set(self.required_columns)
            if extra:
                failures.append(self._create_failure(
                    f"Unexpected columns: {sorted(extra)}",
                    details={"extra_columns": list(extra)}
                ))

        # Check full schema if provided
        if self.expected_schema:
            expected_fields = {f.name: f.dataType for f in self.expected_schema.fields}
            actual_fields = {f.name: f.dataType for f in df.schema.fields}

            for field_name, expected_type in expected_fields.items():
                if field_name not in actual_fields:
                    failures.append(self._create_failure(
                        f"Missing field: {field_name}",
                        column=field_name
                    ))
                elif actual_fields[field_name] != expected_type:
                    failures.append(self._create_failure(
                        f"Type mismatch for {field_name}",
                        column=field_name,
                        details={
                            "expected": str(expected_type),
                            "actual": str(actual_fields[field_name])
                        }
                    ))

        return ValidationResult(
            passed=len(failures) == 0,
            failures=failures
        )


class UniquenessValidation(ValidationRule):
    """Validate uniqueness constraints on columns."""

    def __init__(
        self,
        columns: List[str],
        name: Optional[str] = None
    ):
        """Initialize uniqueness validation.

        Args:
            columns: Columns that should have unique combinations
            name: Rule name
        """
        super().__init__(name or f"UniquenessValidation({','.join(columns)})")
        self.columns = columns

    def validate(self, df: DataFrame) -> ValidationResult:
        """Validate uniqueness."""
        failures = []

        # Check if columns exist
        missing_cols = set(self.columns) - set(df.columns)
        if missing_cols:
            failures.append(self._create_failure(
                f"Columns not found: {sorted(missing_cols)}",
                details={"missing_columns": list(missing_cols)}
            ))
            return ValidationResult(passed=False, failures=failures)

        # Count total rows
        total_count = df.count()

        # Count distinct combinations
        distinct_count = df.select(self.columns).distinct().count()

        if total_count != distinct_count:
            duplicate_count = total_count - distinct_count
            failures.append(self._create_failure(
                f"Found {duplicate_count} duplicate rows on columns {self.columns}",
                details={
                    "total_rows": total_count,
                    "unique_rows": distinct_count,
                    "duplicate_rows": duplicate_count
                }
            ))

        return ValidationResult(
            passed=len(failures) == 0,
            failures=failures,
            metadata={
                "total_rows": total_count,
                "unique_rows": distinct_count
            }
        )


class RangeValidation(ValidationRule):
    """Validate that column values are within expected ranges."""

    def __init__(
        self,
        column: str,
        min_value: Optional[Any] = None,
        max_value: Optional[Any] = None,
        allow_null: bool = True,
        name: Optional[str] = None
    ):
        """Initialize range validation.

        Args:
            column: Column to validate
            min_value: Minimum allowed value (inclusive)
            max_value: Maximum allowed value (inclusive)
            allow_null: Whether null values are allowed
            name: Rule name
        """
        super().__init__(name or f"RangeValidation({column})")
        self.column = column
        self.min_value = min_value
        self.max_value = max_value
        self.allow_null = allow_null

    def validate(self, df: DataFrame) -> ValidationResult:
        """Validate range."""
        from pyspark.sql.functions import col, count, when

        failures = []

        if self.column not in df.columns:
            failures.append(self._create_failure(
                f"Column '{self.column}' not found",
                column=self.column
            ))
            return ValidationResult(passed=False, failures=failures)

        # Check for nulls if not allowed
        if not self.allow_null:
            null_count = df.filter(col(self.column).isNull()).count()
            if null_count > 0:
                failures.append(self._create_failure(
                    f"Found {null_count} null values in '{self.column}'",
                    column=self.column,
                    details={"null_count": null_count}
                ))

        # Check min value
        if self.min_value is not None:
            below_min = df.filter(
                col(self.column).isNotNull() & (col(self.column) < self.min_value)
            ).count()

            if below_min > 0:
                failures.append(self._create_failure(
                    f"Found {below_min} values below minimum {self.min_value} in '{self.column}'",
                    column=self.column,
                    details={"below_min_count": below_min, "min_value": self.min_value}
                ))

        # Check max value
        if self.max_value is not None:
            above_max = df.filter(
                col(self.column).isNotNull() & (col(self.column) > self.max_value)
            ).count()

            if above_max > 0:
                failures.append(self._create_failure(
                    f"Found {above_max} values above maximum {self.max_value} in '{self.column}'",
                    column=self.column,
                    details={"above_max_count": above_max, "max_value": self.max_value}
                ))

        return ValidationResult(
            passed=len(failures) == 0,
            failures=failures
        )


class NullCheckValidation(ValidationRule):
    """Validate null values in columns."""

    def __init__(
        self,
        columns: List[str],
        allow_null: bool = False,
        max_null_percentage: Optional[float] = None,
        name: Optional[str] = None
    ):
        """Initialize null check validation.

        Args:
            columns: Columns to check
            allow_null: Whether nulls are allowed
            max_null_percentage: Maximum percentage of nulls allowed (0-100)
            name: Rule name
        """
        super().__init__(name or "NullCheckValidation")
        self.columns = columns
        self.allow_null = allow_null
        self.max_null_percentage = max_null_percentage

    def validate(self, df: DataFrame) -> ValidationResult:
        """Validate nulls."""
        from pyspark.sql.functions import col, sum as _sum, count

        failures = []
        total_rows = df.count()

        for column in self.columns:
            if column not in df.columns:
                failures.append(self._create_failure(
                    f"Column '{column}' not found",
                    column=column
                ))
                continue

            null_count = df.filter(col(column).isNull()).count()
            null_percentage = (null_count / total_rows * 100) if total_rows > 0 else 0

            if not self.allow_null and null_count > 0:
                failures.append(self._create_failure(
                    f"Column '{column}' contains {null_count} null values ({null_percentage:.2f}%)",
                    column=column,
                    details={
                        "null_count": null_count,
                        "null_percentage": null_percentage
                    }
                ))
            elif self.max_null_percentage is not None and null_percentage > self.max_null_percentage:
                failures.append(self._create_failure(
                    f"Column '{column}' has {null_percentage:.2f}% nulls, exceeds limit of {self.max_null_percentage}%",
                    column=column,
                    details={
                        "null_count": null_count,
                        "null_percentage": null_percentage,
                        "max_allowed": self.max_null_percentage
                    }
                ))

        return ValidationResult(
            passed=len(failures) == 0,
            failures=failures
        )


class RegexValidation(ValidationRule):
    """Validate column values match a regex pattern."""

    def __init__(
        self,
        column: str,
        pattern: str,
        match_required: bool = True,
        name: Optional[str] = None
    ):
        """Initialize regex validation.

        Args:
            column: Column to validate
            pattern: Regex pattern to match
            match_required: Whether all values must match pattern
            name: Rule name
        """
        super().__init__(name or f"RegexValidation({column})")
        self.column = column
        self.pattern = pattern
        self.match_required = match_required

    def validate(self, df: DataFrame) -> ValidationResult:
        """Validate regex pattern."""
        from pyspark.sql.functions import col

        failures = []

        if self.column not in df.columns:
            failures.append(self._create_failure(
                f"Column '{self.column}' not found",
                column=self.column
            ))
            return ValidationResult(passed=False, failures=failures)

        # Filter rows that don't match pattern
        non_matching = df.filter(
            col(self.column).isNotNull() & ~col(self.column).rlike(self.pattern)
        ).count()

        if self.match_required and non_matching > 0:
            failures.append(self._create_failure(
                f"Found {non_matching} values not matching pattern '{self.pattern}' in '{self.column}'",
                column=self.column,
                details={
                    "non_matching_count": non_matching,
                    "pattern": self.pattern
                }
            ))

        return ValidationResult(
            passed=len(failures) == 0,
            failures=failures
        )


class CustomValidation(ValidationRule):
    """Custom validation using a user-defined function."""

    def __init__(
        self,
        validation_func: Callable[[DataFrame], ValidationResult],
        name: Optional[str] = None
    ):
        """Initialize custom validation.

        Args:
            validation_func: Function that takes DataFrame and returns ValidationResult
            name: Rule name
        """
        super().__init__(name or "CustomValidation")
        self.validation_func = validation_func

    def validate(self, df: DataFrame) -> ValidationResult:
        """Run custom validation."""
        return self.validation_func(df)
