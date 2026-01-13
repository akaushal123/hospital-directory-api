from fastapi import FastAPI

from src.hospital_bulk_api.api import hospital_bulk_api

app = FastAPI(title="Hospital Bulk Processing API")
app.include_router(hospital_bulk_api)