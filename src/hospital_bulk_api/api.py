from fastapi import UploadFile, File, HTTPException, APIRouter
from src.hospital_bulk_api.csv import parse_and_validate_csv, CSVValidationError, validate_csv_only
from src.hospital_bulk_api.processing import process_bulk_hospitals, resume_bulk_hospitals
from src.hospital_bulk_api.schemas import BulkResponse
from src.hospital_bulk_api.store import progress_store

hospital_bulk_api = APIRouter(prefix="/hospitals/bulk")

@hospital_bulk_api.post("/validate")
async def validate_bulk_csv(file: UploadFile = File(...)):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed")

    contents = (await file.read()).decode("utf-8")

    result = validate_csv_only(contents)
    return result


@hospital_bulk_api.post("", response_model=BulkResponse)
async def bulk_create_hospitals(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed")

    contents = (await file.read()).decode("utf-8")

    try:
        rows = parse_and_validate_csv(contents)
    except CSVValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    result = await process_bulk_hospitals(rows)

    return BulkResponse(
        batch_id=result["batch_id"],
        total_hospitals=result["total"],
        processed_hospitals=result["success"],
        failed_hospitals=result["failed"],
        processing_time_seconds=result["time"],
        batch_activated=result["activated"],
        hospitals=result["results"]
    )


@hospital_bulk_api.get("/{batch_id}/status")
async def get_bulk_status(batch_id: str):
    if batch_id not in progress_store:
        raise HTTPException(status_code=404, detail="Batch not found")

    return progress_store[batch_id]


@hospital_bulk_api.post("/{batch_id}/resume")
async def resume_bulk(batch_id: str):
    try:
        return await resume_bulk_hospitals(batch_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
