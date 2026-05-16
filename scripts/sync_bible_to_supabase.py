"""
sync_bible_to_supabase.py — Pull V2RETAIL.GOLD bible tables into Supabase bible.*

Prereq:
  pip install snowflake-connector-python psycopg2-binary cryptography
  env SUPABASE_DB_URL = postgresql://postgres.<ref>:<password>@<host>:5432/postgres  (session-mode, port 5432)

Run schema first: psql $SUPABASE_DB_URL -f bible_supabase_migration.sql
Then: python sync_bible_to_supabase.py
"""
import csv
import io
import os
import sys
import time

import psycopg2

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sf_conn import connect as sf_connect

TABLES = [
    ("bible.dim_grid_div",            "V2RETAIL.GOLD.DIM_GRID_DIV"),
    ("bible.dim_fg_maj_cat",          "V2RETAIL.GOLD.DIM_FG_MAJ_CAT"),
    ("bible.dim_mvgr_family",         "V2RETAIL.GOLD.DIM_MVGR_FAMILY"),
    ("bible.dim_mvgr_value",          "V2RETAIL.GOLD.DIM_MVGR_VALUE"),
    ("bible.dim_mvgr_synonym",        "V2RETAIL.GOLD.DIM_MVGR_SYNONYM"),
    ("bible.dim_mvgr_required_by_fg", "V2RETAIL.GOLD.DIM_MVGR_REQUIRED_BY_FG"),
]


def get_pg_cols(pg_cur, qualified_table):
    schema, table = qualified_table.split(".")
    pg_cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position",
        (schema, table),
    )
    return [r[0] for r in pg_cur.fetchall()]


def sync_one(sf_cur, pg_conn, pg_table, sf_table):
    pg_cur = pg_conn.cursor()
    pg_cols = get_pg_cols(pg_cur, pg_table)
    pg_col_set = {c.lower() for c in pg_cols}

    sf_cur.execute(f"DESC TABLE {sf_table}")
    sf_desc = sf_cur.fetchall()
    sf_cols = [r[0] for r in sf_desc]
    sf_types = {r[0]: r[1] for r in sf_desc}
    common_cols = [c for c in sf_cols if c.lower() in pg_col_set]
    print(f"\n>>> {pg_table}  <<  {sf_table}")
    print(f"    cols pg={len(pg_cols)} sf={len(sf_cols)} common={len(common_cols)}")

    # Skip TIMESTAMP cols entirely (some are corrupt from initial write_pandas load
    # — Postgres DEFAULT NOW() fills them on load). Cast VARIANT to string.
    skip_cols = set()
    sel_exprs = []
    keep_cols = []
    for c in common_cols:
        t = sf_types.get(c, '').upper()
        if 'TIMESTAMP' in t:
            skip_cols.add(c)
            continue
        if 'VARIANT' in t:
            sel_exprs.append(f"TO_VARCHAR({c}) AS {c}")
        else:
            sel_exprs.append(c)
        keep_cols.append(c)
    common_cols = keep_cols
    if skip_cols: print(f"    skipping TIMESTAMP cols (Postgres DEFAULT NOW() fills): {sorted(skip_cols)}")
    col_list = ", ".join(sel_exprs)
    # Dedupe synonym table on syn_id (hash collisions in Snowflake-side seed)
    distinct = "DISTINCT " if pg_table == "bible.dim_mvgr_synonym" else ""
    sf_cur.execute(f"SELECT {distinct}{col_list} FROM {sf_table} {'QUALIFY ROW_NUMBER() OVER (PARTITION BY SYN_ID ORDER BY ALIAS)=1' if pg_table == 'bible.dim_mvgr_synonym' else ''}")
    rows = sf_cur.fetchall()
    print(f"    pulled {len(rows):,} rows from Snowflake")

    pg_cur.execute(f"TRUNCATE TABLE {pg_table}")
    buf = io.StringIO()
    w = csv.writer(buf, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
    for r in rows:
        w.writerow(["" if v is None else v for v in r])
    buf.seek(0)
    pg_col_list = ", ".join(c.lower() for c in common_cols)
    pg_cur.copy_expert(
        f"COPY {pg_table} ({pg_col_list}) FROM STDIN WITH (FORMAT csv, NULL '')",
        buf,
    )
    pg_conn.commit()
    print(f"    loaded {len(rows):,} rows to Supabase")
    pg_cur.close()


def main():
    db_url = os.environ.get("SUPABASE_DB_URL")
    if not db_url:
        print("ERROR: set SUPABASE_DB_URL env var (session-mode pooler, port 5432)")
        sys.exit(1)

    sf = sf_connect()
    sf_cur = sf.cursor()
    # Disable nanoarrow result format — hits "int too large" on some bigints
    sf_cur.execute("ALTER SESSION SET PYTHON_CONNECTOR_QUERY_RESULT_FORMAT='JSON'")
    pg = psycopg2.connect(db_url)
    pg.autocommit = False

    t0 = time.time()
    for pg_table, sf_table in TABLES:
        sync_one(sf_cur, pg, pg_table, sf_table)
    print(f"\n>>> done in {time.time()-t0:.1f}s")

    sf_cur.close()
    sf.close()
    pg.close()


if __name__ == "__main__":
    main()
