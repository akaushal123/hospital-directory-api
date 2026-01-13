import time
import uuid
import asyncio
from typing import List

from .clients import HospitalAPIClient
from .schemas import HospitalResult
from .store import progress_store, failed_rows_store


async def _create_single_hospital(
    client: HospitalAPIClient,
    row: dict,
    batch_id: str
):
    """
    Worker function to create a single hospital.
    Returns (row, response_json) on success.
    Raises exception on failure.
    """
    payload = {
        "name": row["name"],
        "address": row["address"],
        "phone": row["phone"],
        "creation_batch_id": batch_id
    }
    response = await client.create_hospital(payload)
    return {
        "row": row,
        "response": response
    }

async def _retry_single_hospital(
    client: HospitalAPIClient,
    row: dict,
    batch_id: str
):
    """
    Retry worker that preserves row identity.
    """
    payload = {
        "name": row["name"],
        "address": row["address"],
        "phone": row["phone"],
        "creation_batch_id": batch_id
    }

    response = await client.create_hospital(payload)
    return {
        "row": row,
        "response": response
    }


async def process_bulk_hospitals(rows: List[dict]) -> dict:
    """
    Orchestrates bulk hospital creation with:
    - concurrency
    - retry safety
    - progress tracking
    - conditional batch activation
    """

    start_time = time.time()
    batch_id = str(uuid.uuid4())
    client = HospitalAPIClient()

    total = len(rows)
    success_count = 0
    failed_count = 0
    results: List[HospitalResult] = []

    # Initialize progress tracking
    progress_store[batch_id] = {
        "total": total,
        "processed": 0,
        "failed": 0,
        "status": "processing"
    }

    # Create concurrent tasks
    tasks = [
        _create_single_hospital(client, row, batch_id)
        for row in rows
    ]

    responses = await asyncio.gather(*tasks, return_exceptions=True)

    for index, item in enumerate(responses):
        original_row = rows[index]

        if isinstance(item, Exception):
            failed_count += 1
            progress_store[batch_id]["failed"] += 1
            progress_store[batch_id]["processed"] += 1
            failed_rows_store.setdefault(batch_id, []).append(original_row)

            results.append(HospitalResult(
                row=original_row["row"],
                name=original_row["name"],
                hospital_id=None,
                status="failed",
                error=str(item)
            ))

        else:
            success_count += 1
            progress_store[batch_id]["processed"] += 1

            results.append(HospitalResult(
                row=original_row["row"],
                name=original_row["name"],
                hospital_id=item["response"]["id"],
                status="created"
            ))

    # Activate batch ONLY if all hospitals succeeded
    batch_activated = False
    if success_count == total:
        await client.activate_batch(batch_id)
        batch_activated = True
        for r in results:
            r.status = "created_and_activated"
        progress_store[batch_id]["status"] = "completed"
    else:
        progress_store[batch_id]["status"] = "completed_with_errors"

    end_time = time.time()

    return {
        "batch_id": batch_id,
        "total": total,
        "success": success_count,
        "failed": failed_count,
        "time": round(end_time - start_time, 2),
        "activated": batch_activated,
        "results": results
    }

async def resume_bulk_hospitals(batch_id: str) -> dict:
    """
        Retries only failed rows for a given batch_id.
        Concurrency-safe and idempotency-aware.
    """
    if batch_id not in failed_rows_store or not failed_rows_store[batch_id]:
        raise ValueError("No failed rows available for resume")

    rows_to_retry: List[dict] = failed_rows_store[batch_id]
    client = HospitalAPIClient()

    success_count = 0
    failed_count = 0
    results: List[HospitalResult] = []

    # Create retry tasks with preserved row context
    tasks = [
        _retry_single_hospital(client, row, batch_id)
        for row in rows_to_retry
    ]

    responses = await asyncio.gather(*tasks, return_exceptions=True)

    still_failed_rows: List[dict] = []

    for index, item in enumerate(responses):
        original_row = rows_to_retry[index]

        if isinstance(item, Exception):
            failed_count += 1
            still_failed_rows.append(original_row)

            results.append(HospitalResult(
                row=original_row["row"],
                name=original_row["name"],
                hospital_id=None,
                status="failed",
                error=str(item)
            ))
        else:
            success_count += 1

            results.append(HospitalResult(
                row=original_row["row"],
                name=original_row["name"],
                hospital_id=item["response"]["id"],
                status="created"
            ))

    # Update failed rows store
    if still_failed_rows:
        failed_rows_store[batch_id] = still_failed_rows
        progress_store[batch_id]["status"] = "completed_with_errors"
    else:
        # All retries succeeded
        del failed_rows_store[batch_id]

        await client.activate_batch(batch_id)
        progress_store[batch_id]["status"] = "completed"

    return {
        "batch_id": batch_id,
        "retried": len(rows_to_retry),
        "success": success_count,
        "failed": failed_count,
        "batch_activated": batch_id not in failed_rows_store,
        "results": results
    }

