# ================ preflight DB, run_id, quarantine-by-chunk, retry=====================
import logging, uuid
from tenacity import retry, stop_after_attempt, wait_exponential
from sqlalchemy.exc import OperationalError
from .config import LOG_LEVEL
from .extract import extract
from .transform import transform
from .load import get_engine, create_table, upsert

logger = logging.getLogger("etl")
logger.setLevel(LOG_LEVEL)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(handler)

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=20))
def _safe_upsert(engine, df):
    upsert(engine, df)

def run_pipeline():
    run_id = uuid.uuid4().hex[:8]
    logger.info(f"ETL started (run_id={run_id})")
    engine = get_engine()

    # Preflight DB connection
    try:
        with engine.connect():
            pass
    except OperationalError:
        logger.error("Cannot connect to DB. Check DB_URL or start the DB service.")
        raise

    create_table(engine)

    for i, df in enumerate(extract(), start=1):
        logger.info(f"Chunk {i}: rows={len(df)}")
        if df.empty:
            continue

        try:
            tdf = transform(df, run_id=run_id, chunk_idx=i)
        except Exception:
            # quarantined in transform(); skip this chunk
            continue

        try:
            _safe_upsert(engine, tdf)
            logger.info(f"Loaded chunk {i}: rows={len(tdf)}")
        except Exception as e:
            logger.exception(f"Load failed on chunk {i}: {e}")
            # Don't stop the whole job — continue with the next chunk
            continue

    logger.info("ETL finished")

if __name__ == "__main__":
    run_pipeline()
