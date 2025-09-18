# Exploratory charts
#   1) Salary distribution by position (boxplot)
#   2) City × Big-group heatmap (job counts)
#   3) Trend of hot technologies by month (from job titles)

# Notes:
# - Uses ONLY matplotlib (no seaborn), one chart per figure, no explicit colors.
# - Self-contained: can run on CSV alone. If DB_URL is present -> enable load_from_db()

import os
import re
import math
import pathlib
import numpy as np
import pandas as pd

import matplotlib
matplotlib.use("Agg")  # non-interactive backend for cron/systemd
import matplotlib.pyplot as plt

from dotenv import load_dotenv

# --- Reuse your ETL code ---
from ETL.load import get_engine          # uses DB_URL in your .env
from ETL.transform import (
    parse_salary,                        # salary parser (if DB lacks min/max/unit)
    _job_group,                          # fine-grained job group
    collapse_map,                        # fine->big group mapping (8 buckets)
)

load_dotenv()  # read .env at project root
OUT_DIR = pathlib.Path("reports")
OUT_DIR.mkdir(exist_ok=True)

# Optional FX: convert USD to VND so we can compare in one currency
FX_USD_TO_VND = float(os.getenv("FX_USD_TO_VND", "25000"))


# ---------- DB read (prefer already-normalized columns) ----------
def read_jobs_from_db() -> pd.DataFrame:
    """Read the jobs table created by ETL.
    Columns we try to use (if present): job_title, job_title_group, job_title_group_big,
    salary, min_salary, max_salary, salary_unit, city, district, created_date/created_at.
    """
    engine = get_engine()
    with engine.connect() as conn:
        # Try to read smartly only the columns we need; fall back to SELECT *.
        wanted = [
            "id_hash", "job_title", "job_title_group", "job_title_group_big",
            "salary", "min_salary", "max_salary", "salary_unit",
            "address", "city", "district",
            "created_date", "created_at"  # one of these may exist
        ]
        try:
            # Get actual column names from DB
            cols = pd.read_sql("SELECT * FROM jobs LIMIT 1", conn).columns.tolist()
            select_cols = [c for c in wanted if c in cols]
            if not select_cols:  # safety
                select_cols = cols
            df = pd.read_sql(f"SELECT {', '.join(select_cols)} FROM jobs", conn)
        except Exception:
            # Fallback: just read everything
            df = pd.read_sql("SELECT * FROM jobs", conn)

    # Build month if we have any time column
    time_col = "created_date" if "created_date" in df.columns else ("created_at" if "created_at" in df.columns else None)
    if time_col:
        df["created_date"] = pd.to_datetime(df[time_col], errors="coerce")
        df["month"] = df["created_date"].dt.to_period("M").astype(str)
    else:
        df["month"] = None
    return df


# ---------- Fill missing normalized columns by reusing ETL helpers ----------
def ensure_normalized(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure the DataFrame has the normalized columns needed for plotting.
    Only compute what is missing to avoid rework.
    """
    out = df.copy()

    # 1) Job groups
    if "job_title_group" not in out.columns:
        out["job_title_group"] = out["job_title"].apply(_job_group)
    if "job_title_group_big" not in out.columns:
        out["job_title_group_big"] = out["job_title_group"].map(collapse_map).fillna("other")

    # 2) Salary min/max/unit
    need_salary_parse = any(col not in out.columns for col in ["min_salary", "max_salary", "salary_unit"])
    if need_salary_parse and "salary" in out.columns:
        parsed = out["salary"].apply(parse_salary)
        out["min_salary"], out["max_salary"], out["salary_unit"], out["salary_note"] = zip(*parsed)

    # 3) City: prefer the column already built by ETL; if not present,
    #    you can import and use your split_address_joined here.
    if "city" not in out.columns and "address" in out.columns:
        # Minimal fallback: take token before ":"; you already have a richer version in ETL.
        out["city"] = out["address"].astype(str).str.split(":", n=1, expand=True)[0].str.strip()

    return out


# ---------- Salary helpers ----------
def salary_point_vnd(row: pd.Series) -> float:
    """Pick a single numeric salary in VND for plotting.
    - If both min & max exist -> midpoint
    - Else whichever exists
    - Convert USD to VND with FX_USD_TO_VND
    """
    a, b = row.get("min_salary"), row.get("max_salary")
    unit = str(row.get("salary_unit") or "VND").upper()
    val = None
    if pd.notna(a) and pd.notna(b):
        val = (float(a) + float(b)) / 2.0
    elif pd.notna(a):
        val = float(a)
    elif pd.notna(b):
        val = float(b)
    if val is None or np.isnan(val):
        return np.nan
    if unit == "USD":
        return val * FX_USD_TO_VND
    return val


# ---------- Charts ----------
def plot_salary_boxplot(df: pd.DataFrame, out_path: pathlib.Path):
    d = df.copy()
    d["salary_vnd"] = d.apply(salary_point_vnd, axis=1)
    d = d[pd.notna(d["salary_vnd"]) & (d["salary_vnd"] > 0)]
    if d.empty:
        print("[salary] No numeric salary to plot.")
        return

    # Clip to 1–99% to reduce extreme outliers
    lo, hi = d["salary_vnd"].quantile([0.01, 0.99]).tolist()
    d["salary_clip"] = d["salary_vnd"].clip(lo, hi)
    order = d.groupby("job_title_group_big")["salary_clip"].median().sort_values(ascending=False).index.tolist()
    data = [d.loc[d["job_title_group_big"] == g, "salary_clip"].values for g in order]

    plt.figure(figsize=(11, 6))
    plt.boxplot(data, showmeans=True)
    plt.xticks(range(1, len(order) + 1), order, rotation=20, ha="right")
    plt.title("Salary distribution by big job group (VND)")
    plt.ylabel("VND")
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()
    print(f"[salary] Saved -> {out_path}")


def plot_city_group_heatmap(df: pd.DataFrame, out_path: pathlib.Path, top_n_cities: int = 10):
    if "city" not in df.columns:
        print("[heatmap] Missing 'city' column.")
        return
    heat = (df[df["city"].notna()]
              .assign(job_title_group_big=df["job_title_group_big"].fillna("other"))
              .pivot_table(index="city",
                           columns="job_title_group_big",
                           values="job_title",
                           aggfunc="count",
                           fill_value=0))
    # Keep top cities by total jobs
    heat = heat.loc[heat.sum(axis=1).sort_values(ascending=False).head(top_n_cities).index]
    if heat.empty:
        print("[heatmap] Nothing to plot.")
        return

    arr = heat.values
    plt.figure(figsize=(12, 6))
    plt.imshow(arr, aspect="auto")
    plt.xticks(range(heat.shape[1]), list(heat.columns), rotation=45, ha="right")
    plt.yticks(range(heat.shape[0]), list(heat.index))
    plt.title("Job distribution heatmap: City × Big Group (counts)")
    plt.colorbar()
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()
    print(f"[heatmap] Saved -> {out_path}")


def plot_tech_trend(df: pd.DataFrame, out_line: pathlib.Path, out_bar_fallback: pathlib.Path):
    """If df['month'] exists (built from created_date/created_at) → line chart by month.
       Else → fallback to total counts (bar chart).
    """
    tech_patterns = {
        "python": r"\bpython\b",
        "java": r"\bjava\b",
        "javascript": r"\bjavascript\b|\bjs\b",
        "typescript": r"\btypescript\b|\bts\b",
        "react": r"\breact\b|\breactjs\b|\breact native\b",
        "nodejs": r"\bnodejs\b|\bnode\b",
        ".net/c#": r"\bdotnet\b|\bcsharp\b|\.net",
        "php": r"\bphp\b",
        "golang": r"\bgolang\b|\bgo\b",
        "docker": r"\bdocker\b",
        "kubernetes": r"\bkubernetes\b|\bk8s\b",
        "aws": r"\baws\b",
        "azure": r"\bazure\b",
        "gcp": r"\bgcp\b",
        "sql": r"\bsql\b|\bpostgres\b|\bmysql\b|\boracle\b",
    }

    def has_pat(text, pat): return bool(re.search(pat, str(text).lower()))

    if "month" in df.columns and df["month"].notna().any():
        trend = []
        for tech, pat in tech_patterns.items():
            tmp = (df.assign(hit=df["job_title"].apply(lambda x: has_pat(x, pat)))
                     .groupby("month")["hit"].sum()
                     .rename(tech))
            trend.append(tmp)
        trend_df = pd.concat(trend, axis=1).fillna(0).astype(int)
        if trend_df.empty:
            print("[trend] No data to plot.")
            return
        top6 = trend_df.sum().sort_values(ascending=False).head(6).index.tolist()
        trend_top = trend_df[top6]

        plt.figure(figsize=(12, 6))
        for tech in trend_top.columns:
            plt.plot(trend_top.index, trend_top[tech].values, marker="o", label=tech)
        plt.title("Hot technologies trend by month (from job titles)")
        plt.xlabel("Month"); plt.ylabel("Job count mentioning tech")
        plt.xticks(rotation=45, ha="right")
        plt.legend()
        plt.tight_layout()
        plt.savefig(out_line)
        plt.close()
        print(f"[trend] Saved -> {out_line}")
    else:
        # Fallback: total counts as a bar chart
        counts = {}
        titles = df["job_title"].astype(str).tolist()
        all_text = "\n".join(titles).lower()
        for tech, pat in tech_patterns.items():
            counts[tech] = len(re.findall(pat, all_text))
        s = pd.Series(counts).sort_values(ascending=False).head(10)

        plt.figure(figsize=(12, 6))
        plt.bar(s.index, s.values)
        plt.title("Hot technologies (total counts) — fallback (no time column in DB)")
        plt.ylabel("Job count mentioning tech")
        plt.xticks(rotation=30, ha="right")
        plt.tight_layout()
        plt.savefig(out_bar_fallback)
        plt.close()
        print(f"[trend/fallback] Saved -> {out_bar_fallback}")
        print("Hint: add created_date/created_at in ETL to unlock monthly trend.")


# ---------- Main ----------
def main():
    df = read_jobs_from_db()
    df = ensure_normalized(df)   # reuse ETL helpers only if something is missing

    # 1) Salary boxplot (by big group)
    plot_salary_boxplot(df, OUT_DIR / "salary_by_group_boxplot.png")

    # 2) Heatmap City × Big Group
    plot_city_group_heatmap(df, OUT_DIR / "heatmap_city_group.png")

    # 3) Tech trend (by month if available; else fallback totals)
    plot_tech_trend(
        df,
        OUT_DIR / "tech_trend_by_month.png",
        OUT_DIR / "tech_top_fallback_bar.png",
    )


if __name__ == "__main__":
    main()