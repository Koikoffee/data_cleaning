# tests/test_db_integration.py
import os
import pytest
from sqlalchemy import text
from ETL.load import get_engine

@pytest.mark.integration
def test_db_connection():
    if not os.getenv("DB_URL"):
        pytest.skip("DB_URL not set")
    engine = get_engine()
    with engine.connect() as conn:
        # SQLAlchemy 2.x needs text() or use exec_driver_sql()
        val = conn.execute(text("SELECT 1")).scalar_one()
        assert val == 1
