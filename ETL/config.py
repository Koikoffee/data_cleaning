import os
from dotenv import load_dotenv
load_dotenv()

CSV_PATH  = os.getenv("CSV_PATH", "./data/data.csv")
DB_URL    = os.getenv("DB_URL", "postgresql+psycopg2://user:pass@localhost:5432/it_jobs")
CHUNKSIZE = int(os.getenv("CHUNKSIZE", "0")) or None
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Error-handling config
STRICT_SCHEMA   = os.getenv("STRICT_SCHEMA", "true").lower() == "true"
REQUIRED_COLUMNS = ["job_title", "address", "salary"]        # tối thiểu để ETL chạy
QUARANTINE_DIR  = os.getenv("QUARANTINE_DIR", "./quarantine")
os.makedirs(QUARANTINE_DIR, exist_ok=True)

