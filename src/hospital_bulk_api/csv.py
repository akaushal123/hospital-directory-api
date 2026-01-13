import csv
from io import StringIO

MAX_ROWS = 20
REQUIRED_COLUMNS = {"name", "address"}

class CSVValidationError(Exception):
    pass


def parse_and_validate_csv(contents: str):
    reader = csv.DictReader(StringIO(contents))

    if not REQUIRED_COLUMNS.issubset(reader.fieldnames):
        raise CSVValidationError("CSV must contain name and address columns")

    rows = list(reader)

    if len(rows) == 0:
        raise CSVValidationError("CSV is empty")

    if len(rows) > MAX_ROWS:
        raise CSVValidationError("CSV exceeds 20 hospital limit")

    validated_rows = []
    for idx, row in enumerate(rows, start=1):
        if not row.get("name") or not row.get("address"):
            raise CSVValidationError(f"Row {idx} missing required fields")
        validated_rows.append({
            "row": idx,
            "name": row["name"].strip(),
            "address": row["address"].strip(),
            "phone": row.get("phone", "").strip() or None
        })

    return validated_rows

def validate_csv_only(contents: str):
    """
    Validates CSV structure and content without side effects.
    Returns structured validation results.
    """
    reader = csv.DictReader(StringIO(contents))

    errors = []
    rows = []

    if not reader.fieldnames:
        return {
            "valid": False,
            "errors": ["CSV has no headers"],
            "total_rows": 0,
            "valid_rows": 0
        }

    if not REQUIRED_COLUMNS.issubset(reader.fieldnames):
        return {
            "valid": False,
            "errors": ["CSV must contain name and address columns"],
            "total_rows": 0,
            "valid_rows": 0
        }

    for idx, row in enumerate(reader, start=1):
        row_errors = []

        if not row.get("name"):
            row_errors.append("Missing name")

        if not row.get("address"):
            row_errors.append("Missing address")

        if row_errors:
            errors.append({
                "row": idx,
                "errors": row_errors
            })
        else:
            rows.append(idx)

    if len(rows) > MAX_ROWS:
        return {
            "valid": False,
            "errors": [f"CSV exceeds maximum limit of {MAX_ROWS} hospitals"],
            "total_rows": len(rows),
            "valid_rows": 0
        }

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "total_rows": len(rows) + len(errors),
        "valid_rows": len(rows)
    }

