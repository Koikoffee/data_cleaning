# =============== file does not exist, empty, encoding ==============================================
import pandas as pd
from .config import CSV_PATH, CHUNKSIZE
import logging
logger = logging.getLogger("etl")

def extract():
    #Yield DataFrame chunks (or a single DataFrame) from CSV with basic guards.
    try:
        if CHUNKSIZE:
            for chunk in pd.read_csv(CSV_PATH, encoding="utf-8", chunksize=CHUNKSIZE):
                yield chunk
        else:
            yield pd.read_csv(CSV_PATH, encoding="utf-8")
    except FileNotFoundError as e:
        logger.error(f"CSV not found at {CSV_PATH}")
        raise
    except pd.errors.EmptyDataError:
        logger.error("CSV is empty — nothing to process.")
        yield pd.DataFrame()
    except UnicodeDecodeError:
        logger.error("Encoding error reading CSV (try encoding='utf-8-sig' or correct source).")
        raise
