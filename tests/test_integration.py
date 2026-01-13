from unittest.mock import AsyncMock, patch

from src.hospital_bulk_api.store import progress_store


def test_bulk_create_success(client):
    csv_data = "name,address,phone\nH1,Addr1,111\nH2,Addr2,222\n"

    async def fake_create(payload):
        # simulate server assigning an incremental id
        return {"id": 123}

    async def fake_activate(batch_id):
        return {"status": "activated"}

    with patch("src.hospital_bulk_api.processing.HospitalAPIClient") as MockClient:
        instance = MockClient.return_value
        instance.create_hospital = AsyncMock(side_effect=fake_create)
        instance.activate_batch = AsyncMock(side_effect=fake_activate)

        response = client.post("/hospitals/bulk", files={"file": ("hosp.csv", csv_data)})
        assert response.status_code == 200
        data = response.json()
        assert data["total_hospitals"] == 2
        assert data["processed_hospitals"] == 2
        assert data["failed_hospitals"] == 0
        assert data["batch_activated"] is True

        # Check progress store has the batch
        batch_id = data["batch_id"]
        assert batch_id in progress_store
        assert progress_store[batch_id]["status"] == "completed"

