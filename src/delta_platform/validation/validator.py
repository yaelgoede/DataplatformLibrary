"""Data validator for running multiple validation rules."""

from typing import List
from pyspark.sql import DataFrame

from delta_platform.validation.rules import ValidationRule, ValidationResult, ValidationFailure


class DataValidator:
    """
    Orchestrates multiple validation rules.

    Uses Chain of Responsibility pattern to run validation rules
    in sequence and aggregate results.
    """

    def __init__(self, rules: List[ValidationRule]):
        """Initialize data validator.

        Args:
            rules: List of validation rules to apply
        """
        self.rules = rules

    def validate(self, df: DataFrame, fail_fast: bool = False) -> ValidationResult:
        """Run all validation rules on the DataFrame.

        Args:
            df: DataFrame to validate
            fail_fast: If True, stop on first failure

        Returns:
            Aggregated ValidationResult
        """
        all_failures = []
        all_warnings = []
        all_metadata = {}

        for rule in self.rules:
            result = rule.validate(df)

            # Separate errors and warnings
            errors = [f for f in result.failures if f.severity == "ERROR"]
            warnings = [f for f in result.failures if f.severity == "WARNING"]

            all_failures.extend(errors)
            all_warnings.extend(warnings)
            all_metadata[rule.name] = result.metadata

            # Fail fast if requested and errors found
            if fail_fast and errors:
                break

        passed = len(all_failures) == 0

        return ValidationResult(
            passed=passed,
            failures=all_failures,
            warnings=all_warnings,
            metadata=all_metadata
        )

    def add_rule(self, rule: ValidationRule) -> "DataValidator":
        """Add a validation rule.

        Args:
            rule: Validation rule to add

        Returns:
            Self for method chaining
        """
        self.rules.append(rule)
        return self

    def validate_and_raise(self, df: DataFrame) -> None:
        """Validate and raise exception if validation fails.

        Args:
            df: DataFrame to validate

        Raises:
            ValidationError: If validation fails
        """
        from delta_platform.exceptions import ValidationError

        result = self.validate(df)

        if not result.passed:
            raise ValidationError(result.errors)
