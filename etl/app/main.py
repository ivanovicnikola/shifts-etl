from fastapi import FastAPI, HTTPException
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
API_URL = os.getenv("API_URL", "http://localhost:8000/api/shifts")


@app.post("/run-etl")
async def run_etl():
    """
    Endpoint to trigger the ETL process manually.
    """
    try:
        processor = ShiftDataProcessor(DB_CONFIG, API_URL)
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
