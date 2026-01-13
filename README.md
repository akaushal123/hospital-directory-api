# Hospital Directory API (modularized)

This project provides a FastAPI application to bulk-create hospitals from CSV files.

Structure
- src/hospital_api/: package containing modular components
  - api.py — HTTP routes (FastAPI app)
  - csv.py — CSV parsing/validation
  - clients.py — external Hospital API client
  - processing.py — bulk orchestration logic
  - schemas.py — Pydantic models
  - store.py — in-memory stores

Running
- Install dependencies: ```python -m pip install -r requirements.txt```
- Start server: ```uvicorn src.main:app --reload --port 8000```

Testing
- Run tests with pytest:

```bash
python -m pytest -q
```

