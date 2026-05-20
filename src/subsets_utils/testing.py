"""
Testing utilities for validating transform outputs.

Usage in asset test.py:
    from subsets_utils import validate
    from subsets_utils.testing import assert_valid_year, assert_valid_date, assert_max_length

    def test(table):
        validate(table, {
            "columns": {
                "year": "string",
                "value": "double",
                "country": "string",
            },
            "not_null": ["year", "country"],
            "unique": ["year", "country"],
            "min_rows": 100,
        })

        # Date format validations
        assert_valid_year(table, "year")

        # String validations
        assert_max_length(table, "country", 50)

        # Value range validations
        assert_positive(table, "value")
"""

import re
import pyarrow as pa


# =============================================================================
# Date Format Validators
# =============================================================================

def assert_valid_year(table: pa.Table, column: str) -> None:
    """Assert all non-null values are valid years (YYYY format, 4 digits)."""
    values = [v for v in table.column(column).to_pylist() if v is not None]
    pattern = re.compile(r"^\d{4}$")
    invalid = [v for v in values if not pattern.match(str(v))]
    assert not invalid, f"Column '{column}' has invalid year values: {invalid[:5]}..."


def assert_valid_quarter(table: pa.Table, column: str) -> None:
    """Assert all non-null values are valid quarters (YYYY-QN format)."""
    values = [v for v in table.column(column).to_pylist() if v is not None]
    pattern = re.compile(r"^\d{4}-Q[1-4]$")
    invalid = [v for v in values if not pattern.match(str(v))]
    assert not invalid, f"Column '{column}' has invalid quarter values: {invalid[:5]}..."


def assert_valid_month(table: pa.Table, column: str) -> None:
    """Assert all non-null values are valid months (YYYY-MM format)."""
    values = [v for v in table.column(column).to_pylist() if v is not None]
    pattern = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
    invalid = [v for v in values if not pattern.match(str(v))]
    assert not invalid, f"Column '{column}' has invalid month values: {invalid[:5]}..."


def assert_valid_week(table: pa.Table, column: str) -> None:
    """Assert all non-null values are valid weeks (YYYY-WNN format)."""
    values = [v for v in table.column(column).to_pylist() if v is not None]
    pattern = re.compile(r"^\d{4}-W(0[1-9]|[1-4]\d|5[0-3])$")
    invalid = [v for v in values if not pattern.match(str(v))]
    assert not invalid, f"Column '{column}' has invalid week values: {invalid[:5]}..."


def assert_valid_date(table: pa.Table, column: str) -> None:
    """Assert all non-null values are valid dates (YYYY-MM-DD format)."""
    values = [v for v in table.column(column).to_pylist() if v is not None]
    pattern = re.compile(r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$")
    invalid = [v for v in values if not pattern.match(str(v))]
    assert not invalid, f"Column '{column}' has invalid date values: {invalid[:5]}..."


def assert_valid_date_any(table: pa.Table, column: str) -> None:
    """Assert all non-null values match one of: YYYY, YYYY-QN, YYYY-MM, YYYY-WNN, YYYY-MM-DD."""
    values = [v for v in table.column(column).to_pylist() if v is not None]
    patterns = [
        re.compile(r"^\d{4}$"),  # Year
        re.compile(r"^\d{4}-Q[1-4]$"),  # Quarter
        re.compile(r"^\d{4}-(0[1-9]|1[0-2])$"),  # Month
        re.compile(r"^\d{4}-W(0[1-9]|[1-4]\d|5[0-3])$"),  # Week
        re.compile(r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$"),  # Date
    ]
    invalid = [v for v in values if not any(p.match(str(v)) for p in patterns)]
    assert not invalid, f"Column '{column}' has invalid date values: {invalid[:5]}..."


# =============================================================================
# String Validators
# =============================================================================

def assert_max_length(table: pa.Table, column: str, max_len: int) -> None:
    """Assert all non-null string values have length <= max_len."""
    values = [v for v in table.column(column).to_pylist() if v is not None]
    invalid = [v for v in values if len(str(v)) > max_len]
    assert not invalid, f"Column '{column}' has values exceeding {max_len} chars: {invalid[:5]}..."


def assert_min_length(table: pa.Table, column: str, min_len: int) -> None:
    """Assert all non-null string values have length >= min_len."""
    values = [v for v in table.column(column).to_pylist() if v is not None]
    invalid = [v for v in values if len(str(v)) < min_len]
    assert not invalid, f"Column '{column}' has values shorter than {min_len} chars: {invalid[:5]}..."


def assert_length(table: pa.Table, column: str, exact_len: int) -> None:
    """Assert all non-null string values have exactly the specified length."""
    values = [v for v in table.column(column).to_pylist() if v is not None]
    invalid = [v for v in values if len(str(v)) != exact_len]
    assert not invalid, f"Column '{column}' has values not exactly {exact_len} chars: {invalid[:5]}..."


def assert_matches_pattern(table: pa.Table, column: str, pattern: str, description: str = None) -> None:
    """Assert all non-null values match the regex pattern."""
    values = [v for v in table.column(column).to_pylist() if v is not None]
    regex = re.compile(pattern)
    invalid = [v for v in values if not regex.match(str(v))]
    desc = description or f"pattern '{pattern}'"
    assert not invalid, f"Column '{column}' has values not matching {desc}: {invalid[:5]}..."


def assert_in_set(table: pa.Table, column: str, valid_values: set) -> None:
    """Assert all non-null values are in the set of valid values."""
    values = [v for v in table.column(column).to_pylist() if v is not None]
    invalid = [v for v in values if v not in valid_values]
    assert not invalid, f"Column '{column}' has unexpected values: {invalid[:5]}..."


# =============================================================================
# Numeric Validators
# =============================================================================

def assert_positive(table: pa.Table, column: str, allow_zero: bool = True) -> None:
    """Assert all non-null numeric values are positive (or zero if allow_zero=True)."""
    values = [v for v in table.column(column).to_pylist() if v is not None]
    if allow_zero:
        invalid = [v for v in values if v < 0]
        assert not invalid, f"Column '{column}' has negative values: {invalid[:5]}..."
    else:
        invalid = [v for v in values if v <= 0]
        assert not invalid, f"Column '{column}' has non-positive values: {invalid[:5]}..."


def assert_in_range(table: pa.Table, column: str, min_val: float = None, max_val: float = None) -> None:
    """Assert all non-null numeric values are within the specified range."""
    values = [v for v in table.column(column).to_pylist() if v is not None]
    invalid = []
    for v in values:
        if min_val is not None and v < min_val:
            invalid.append(v)
        elif max_val is not None and v > max_val:
            invalid.append(v)
    range_desc = f"[{min_val}, {max_val}]"
    assert not invalid, f"Column '{column}' has values outside range {range_desc}: {invalid[:5]}..."


def assert_percentage(table: pa.Table, column: str) -> None:
    """Assert all non-null values are valid percentages (0-100)."""
    assert_in_range(table, column, 0, 100)


# =============================================================================
# Schema Validator
# =============================================================================

def validate(table: pa.Table, schema: dict) -> None:
    """Validate table against schema. Raises AssertionError on failure.

    Args:
        table: PyArrow table to validate
        schema: Validation schema with optional keys:
            - columns: dict of {column_name: expected_type_substring}
            - not_null: list of column names that must not have nulls
            - unique: list of column names that form a unique key (composite if multiple)
            - min_rows: minimum expected row count
            - max_rows: maximum expected row count

    Raises:
        AssertionError: If any validation fails
    """
    # Check min/max rows
    if min_rows := schema.get("min_rows"):
        assert len(table) >= min_rows, f"Expected >= {min_rows} rows, got {len(table)}"

    if max_rows := schema.get("max_rows"):
        assert len(table) <= max_rows, f"Expected <= {max_rows} rows, got {len(table)}"

    # Check columns exist and have correct types
    if columns := schema.get("columns"):
        table_columns = set(table.column_names)

        for col, expected_type in columns.items():
            assert col in table_columns, f"Missing column: {col}"
            actual_type = str(table.schema.field(col).type)
            assert expected_type in actual_type, (
                f"Column '{col}': expected type containing '{expected_type}', got '{actual_type}'"
            )

    # Check not-null columns
    if not_null := schema.get("not_null"):
        for col in not_null:
            null_count = table.column(col).null_count
            assert null_count == 0, f"Column '{col}' has {null_count} null values"

    # Check unique constraint (composite key support)
    if unique := schema.get("unique"):
        if isinstance(unique, str):
            unique = [unique]

        if len(unique) == 1:
            values = table.column(unique[0]).to_pylist()
            duplicates = len(values) - len(set(values))
            assert duplicates == 0, f"Column '{unique[0]}' has {duplicates} duplicate values"
        else:
            # Composite key - convert columns once, then zip
            columns_as_lists = [table.column(col).to_pylist() for col in unique]
            rows = list(zip(*columns_as_lists))
            duplicates = len(rows) - len(set(rows))
            assert duplicates == 0, f"Columns {unique} have {duplicates} duplicate combinations"
