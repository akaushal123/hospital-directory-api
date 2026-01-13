import pytest
import asyncio
from unittest.mock import AsyncMock, patch

from src.hospital_bulk_api.processing import process_bulk_hospitals, resume_bulk_hospitals
from src.hospital_bulk_api.store import progress_store, failed_rows_store


@pytest.mark.asyncio
async def test_process_all_success():
    rows = [
        {"row": 1, "name": "A", "address": "addr", "phone": None},
        {"row": 2, "name": "B", "address": "addr2", "phone": None},
    ]

    async def fake_create(payload):
        return {"id": 1}

    async def fake_activate(batch_id):
        return {"status": "activated"}

    # Patch the client used inside processing
    with patch("src.hospital_bulk_api.processing.HospitalAPIClient") as MockClient:
        inst = MockClient.return_value
        inst.create_hospital = AsyncMock(side_effect=fake_create)
        inst.activate_batch = AsyncMock(side_effect=fake_activate)

        result = await process_bulk_hospitals(rows)
        assert result["total"] == 2
        assert result["success"] == 2
        assert result["failed"] == 0
        assert result["activated"] is True


@pytest.mark.asyncio
async def test_process_partial_failure_and_resume():
    # Clear stores
    progress_store.clear()
    failed_rows_store.clear()

    rows = [
        {"row": 1, "name": "A", "address": "addr", "phone": None},
        {"row": 2, "name": "B", "address": "addr2", "phone": None},
    ]

    async def create_success(payload):
        return {"id": 10}

    async def create_fail(payload):
        raise Exception("bad")

    async def fake_activate(batch_id):
        return {"status": "activated"}

    # First run: one success, one failure
    with patch("src.hospital_bulk_api.processing.HospitalAPIClient") as MockClient:
        inst = MockClient.return_value
        inst.create_hospital = AsyncMock(side_effect=[{"id": 10}, Exception("bad")])
        inst.activate_batch = AsyncMock(side_effect=fake_activate)

        result = await process_bulk_hospitals(rows)
        assert result["success"] == 1
        assert result["failed"] == 1
        batch_id = result["batch_id"]
        assert batch_id in failed_rows_store

    # Now patch create to succeed on retry
    with patch("src.hospital_bulk_api.processing.HospitalAPIClient") as MockClient:
        inst = MockClient.return_value
        inst.create_hospital = AsyncMock(side_effect=[{"id": 11}])
        inst.activate_batch = AsyncMock(side_effect=fake_activate)

        resume_result = await resume_bulk_hospitals(batch_id)
        assert resume_result["success"] == 1
        assert resume_result["failed"] == 0
        assert resume_result["batch_activated"] is True
        assert batch_id not in failed_rows_store

