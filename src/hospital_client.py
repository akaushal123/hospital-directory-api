import httpx
import asyncio

BASE_URL = "https://hospital-directory.onrender.com"
MAX_RETRIES = 3
TIMEOUT = 10

class HospitalAPIClient:
    def __init__(self):
        self.client = httpx.AsyncClient(timeout=TIMEOUT)

    async def _request_with_retry(self, method: str, url: str, **kwargs):
        last_exception = None

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = await self.client.request(method, url, **kwargs)
                response.raise_for_status()
                return response.json()
            except Exception as e:
                last_exception = e
                await asyncio.sleep(2 ** attempt)  # exponential backoff

        raise RuntimeError(f"External API failed after retries: {last_exception}")

    async def create_hospital(self, payload: dict):
        return await self._request_with_retry(
            "POST",
            f"{BASE_URL}/hospitals/",
            json=payload
        )

    async def activate_batch(self, batch_id: str):
        return await self._request_with_retry(
            "PATCH",
            f"{BASE_URL}/hospitals/batch/{batch_id}/activate"
        )
