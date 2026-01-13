from pydantic import BaseModel
from typing import List, Optional

class HospitalResult(BaseModel):
    row: int
    name: str
    hospital_id: Optional[int]
    status: str
    error: Optional[str] = None

class BulkResponse(BaseModel):
    batch_id: str
    total_hospitals: int
    processed_hospitals: int
    failed_hospitals: int
    processing_time_seconds: float
    batch_activated: bool
    hospitals: List[HospitalResult]
