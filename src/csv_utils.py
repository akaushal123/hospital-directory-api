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
