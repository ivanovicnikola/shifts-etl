from urllib.parse import parse_qs, urlencode, urlparse, urlunparse
from fastapi import FastAPI, HTTPException, Query
from app.shift_data_processor import ShiftDataProcessor
import os
import logging

app = FastAPI()

# Load database configuration from environment variables
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5433)),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres")
}

# Load API URL from the environment variable
BASE_API_URL = os.getenv("API_URL", "http://localhost:8000/api/shifts")

def add_query_params(url: str, params: dict) -> str:
    """
    Add or update query parameters to a URL.
    """
    url_parts = urlparse(url)
    existing_params = parse_qs(url_parts.query)
    existing_params.update(params)
    new_query = urlencode(existing_params, doseq=True)
    new_url_parts = url_parts._replace(query=new_query)
    return urlunparse(new_url_parts)

@app.post("/run-etl")
async def run_etl(batch_size: int = Query(None, ge=1, le=30)):
    """
    Endpoint to trigger the ETL process.
    Optional query parameter `batch_size` between 1 and 30 determines the number of records.
    """
    try:
        api_url = BASE_API_URL
        if batch_size:
            api_url = add_query_params(BASE_API_URL, {"limit": batch_size})

        processor = ShiftDataProcessor(DB_CONFIG, api_url)
        processor.process_all_pages()

        return {"status": "ETL process completed successfully"}
    except ValueError as e:
        # Specific errors raised in the processing flow
        logging.error(f"ETL process failed: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        # General errors
        logging.error(f"ETL process failed: {e}")
        raise HTTPException(status_code=500, detail=f"ETL process failed: {e}")

@app.post("/clear-data")
async def clear_data():
    """
    Endpoint to clear data from all tables.
    """
    try:
        processor = ShiftDataProcessor(DB_CONFIG, BASE_API_URL)
        processor.clear_data()
        return {"status": "Data cleared successfully"}
    except Exception as e:
        # Log the error and return an HTTP 500 error
        logging.error(f"Failed to clear data: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to clear data: {e}")
