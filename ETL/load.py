# Safe upsert using SQLAlchemy Core. (Define the table once, use upsert in Postgres or MySQL.)
from typing import List, Dict
import hashlib

import pandas as pd
from sqlalchemy import (
    create_engine, MetaData, Table, Column, Text, String, Float
)
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.mysql import insert as my_insert

from .config import DB_URL

# --- DB objects ---
metadata = MetaData()
jobs = Table(
    "jobs", metadata,
    Column("id_hash", String(64), primary_key=True),
    Column("job_title", Text),
    Column("job_title_group", String(64)),
    Column("job_title_group_big", String(32)),
    Column("job_seniority", String(32)),
    Column("salary", Text),
    Column("min_salary", Float),
    Column("max_salary", Float),
    Column("salary_unit", String(8)),
    Column("salary_note", String(32)),
    Column("address", Text),
    Column("city", Text),
    Column("district", Text),
    Column("city_district_pairs_str", Text),
)

def get_engine() -> Engine:
    return create_engine(DB_URL, pool_pre_ping=True)

def create_table(engine: Engine) -> None:
    metadata.create_all(engine)

# --- helpers ---
def make_id_hash(row: pd.Series) -> str:
    key = "|".join([
        str(row.get("job_url", "")),
        str(row.get("job_title", "")),
        str(row.get("company", "")),
        str(row.get("city", "")),
        str(row.get("district", "")),
        str(row.get("salary", "")),
    ])
    return hashlib.sha256(key.encode("utf-8")).hexdigest()

# =========== upsert, NULL/NaN, unique violation ===============================
def _nan_to_none(df: pd.DataFrame) -> pd.DataFrame:
    return df.where(pd.notna(df), None)

def prepare(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "id_hash" not in df.columns:
        df["id_hash"] = df.apply(make_id_hash, axis=1)
    cols: List[str] = [c.key for c in jobs.c]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]
    df = _nan_to_none(df)
    return df

def upsert(engine: Engine, df: pd.DataFrame) -> None:
    if df.empty:
        return
    df = prepare(df)
    # de-dup inside this batch
    df = df.drop_duplicates(subset=["id_hash"], keep="last")
    rows: List[Dict] = df.to_dict(orient="records")
    dialect = engine.dialect.name

    with engine.begin() as conn:
        if dialect == "postgresql":
            ins = pg_insert(jobs)
            update_cols = {c.key: ins.excluded[c.key] for c in jobs.c if c.key != "id_hash"}
            stmt = ins.on_conflict_do_update(index_elements=["id_hash"], set_=update_cols)
            try:
                conn.execute(stmt, rows)
            except Exception as e:
                if "ON CONFLICT DO UPDATE command cannot affect row a second time" in str(e):
                    for r in rows:  # fallback per row
                        conn.execute(stmt, [r])
                else:
                    raise
        elif dialect in ("mysql", "mariadb"):
            ins = my_insert(jobs)
            update_cols = {c.key: ins.inserted[c.key] for c in jobs.c if c.key != "id_hash"}
            stmt = ins.on_duplicate_key_update(**update_cols)
            conn.execute(stmt, rows)
        else:
            df.to_sql("jobs", conn, if_exists="append", index=False, method="multi", chunksize=1000)


