from fastapi import FastAPI, UploadFile, File, HTTPException
from src.csv_utils import parse_and_validate_csv, CSVValidationError
from src.bulk_processor import process_bulk_hospitals
from src.models import BulkResponse
from src.progress_store import progress_store
from src.bulk_processor import resume_bulk_hospitals

app = FastAPI(title="Hospital Bulk Processing API")

@app.post("/hospitals/bulk", response_model=BulkResponse)
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


@app.get("/hospitals/bulk/{batch_id}/status")
async def get_bulk_status(batch_id: str):
    if batch_id not in progress_store:
        raise HTTPException(status_code=404, detail="Batch not found")

    return progress_store[batch_id]


@app.post("/hospitals/bulk/{batch_id}/resume")
async def resume_bulk(batch_id: str):
    try:
        return await resume_bulk_hospitals(batch_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
