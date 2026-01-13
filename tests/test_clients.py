from unittest.mock import AsyncMock, patch

import pytest

from src.hospital_bulk_api.clients import HospitalAPIClient


@pytest.mark.asyncio
async def test_create_hospital_retries_and_fails(monkeypatch):
    # Patch the underlying httpx AsyncClient.request to always raise
    async def always_raise(*args, **kwargs):
        raise Exception("network")

    with patch("src.hospital_bulk_api.clients.httpx.AsyncClient.request", new=AsyncMock(side_effect=always_raise)):
        client = HospitalAPIClient()
        # create_hospital should propagate a RuntimeError after retries
        with pytest.raises(RuntimeError):
            await client.create_hospital({"name": "A"})


@pytest.mark.asyncio
async def test_create_hospital_eventual_success(monkeypatch):
    # First calls raise, final call returns success
    side = [Exception("temp"), Exception("temp2")]

    class MockResponse:
        def __init__(self, data):
            self._data = data

        def raise_for_status(self):
            return None

        def json(self):
            return self._data

    async def side_effect(*args, **kwargs):
        if side:
            result = side.pop(0)
            if isinstance(result, Exception):
                raise result
        # final successful response
        return MockResponse({"id": 99})

    with patch("src.hospital_bulk_api.clients.httpx.AsyncClient.request", new=AsyncMock(side_effect=side_effect)):
        client = HospitalAPIClient()
        res = await client.create_hospital({"name": "A"})
        assert res["id"] == 99

