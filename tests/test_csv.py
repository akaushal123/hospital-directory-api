from src.hospital_bulk_api.csv import parse_and_validate_csv, CSVValidationError


def test_parse_valid_csv():
    csv_data = "name,address,phone\nHospital A,123 Main St,555-0100\n"
    rows = parse_and_validate_csv(csv_data)
    assert len(rows) == 1
    assert rows[0]["name"] == "Hospital A"
    assert rows[0]["address"] == "123 Main St"
    assert rows[0]["phone"] == "555-0100"


def test_missing_columns():
    csv_data = "name,phone\nHospital A,555-0100\n"
    try:
        parse_and_validate_csv(csv_data)
        assert False, "Expected CSVValidationError"
    except CSVValidationError:
        pass


def test_too_many_rows():
    # build CSV with 21 rows
    header = "name,address\n"
    body = "\n".join([f"H{i},Addr {i}" for i in range(21)]) + "\n"
    csv_data = header + body
    try:
        parse_and_validate_csv(csv_data)
        assert False, "Expected CSVValidationError"
    except CSVValidationError:
        pass

