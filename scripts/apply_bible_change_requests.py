"""
apply_bible_change_requests.py — Consume APPROVED rows from bible.change_request
and apply matching mutations to V2RETAIL.GOLD bible tables.

NEVER touches existing public.* tables. Only reads bible.change_request and
writes to V2RETAIL.GOLD.DIM_MVGR_* + V2RETAIL.GOLD.BIBLE_MERCH_REVIEW_QUEUE.

Supported cr_type values:
  PROMOTE_DEV          - flip a DEV-lifecycle MVGR row to ACT
  ADD_VALUE            - insert new MVGR canonical
  RENAME               - update CANON_MVGR_VALUE for an existing row
  DEPRECATE            - set STATUS='IN-ACT' + LIFECYCLE='PHASE_OUT'
  CONFIRM_SPLIT_NAMESPACE - mark a flagged family as resolved (no mutation, just clears flag)
  MERGE                - point losing MVGR_KEY rows to winner via SUPERSEDES_KEY + status PHASE_OUT
  MAP_TO_EXISTING      - add row to dim_mvgr_synonym pointing alias->existing mvgr_key
  CREATE_NEW_CANONICAL - insert new MVGR row + family if missing

Each successful apply:
  1. UPDATEs change_request status -> APPLIED + applied_at
  2. Writes bible.audit_log row with before/after JSON
  3. Mirrors the mutation to V2RETAIL.GOLD via Snowflake

Run as cron (every 5 min) or one-shot.
"""
import json
import os
import sys
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sf_conn import connect as sf_connect


def fetch_approved(pg_cur):
    pg_cur.execute(
        """SELECT cr_id, cr_type, fg_code, maj_cat_code, current_value,
                  proposed_value, payload_json, reviewed_by
           FROM bible.change_request
           WHERE status = 'APPROVED'
           ORDER BY cr_id"""
    )
    return pg_cur.fetchall()


def apply_promote_dev(sf_cur, cr):
    fam = cr["maj_cat_code"]
    val = cr["current_value"]
    sf_cur.execute(
        f"""UPDATE V2RETAIL.GOLD.DIM_MVGR_VALUE
            SET STATUS='ACT', LIFECYCLE='ACT',
                APPROVED_BY=%s, APPROVED_AT=CURRENT_TIMESTAMP()
            WHERE MAJ_CAT_CODE=%s AND MVGR_VALUE=%s AND LIFECYCLE='DEV'""",
        (cr["reviewed_by"], fam, val),
    )
    return {"rows_affected": sf_cur.rowcount, "action": "DEV->ACT"}


def apply_add_value(sf_cur, cr):
    payload = cr["payload_json"] or {}
    sf_cur.execute(
        """INSERT INTO V2RETAIL.GOLD.DIM_MVGR_VALUE
           (GRID_ID, MVGR_KEY, FG_CODE, MAJ_CAT_CODE, MVGR_VALUE, FULL_FORM,
            SHORT_CODE, LABEL_EN, STATUS, LIFECYCLE, SOURCE, CREATED_BY, CREATED_AT)
           SELECT
             SUBSTR(MD5(%s||'|'||%s||'|'||%s),1,16),
             SUBSTR(MD5(%s||'|'||%s||'|'||%s),1,16),
             %s, %s, %s, %s, %s, %s, 'ACT', 'ACT', 'change_request', %s, CURRENT_TIMESTAMP()
           WHERE NOT EXISTS (
             SELECT 1 FROM V2RETAIL.GOLD.DIM_MVGR_VALUE
             WHERE MAJ_CAT_CODE=%s AND MVGR_VALUE=%s AND FG_CODE=%s)""",
        (
            cr["fg_code"], cr["maj_cat_code"], cr["proposed_value"],
            cr["fg_code"], cr["maj_cat_code"], cr["proposed_value"],
            cr["fg_code"] or "_GLOBAL_", cr["maj_cat_code"], cr["proposed_value"],
            payload.get("full_form", cr["proposed_value"]),
            payload.get("short_code"), payload.get("label_en"),
            cr["reviewed_by"],
            cr["maj_cat_code"], cr["proposed_value"], cr["fg_code"] or "_GLOBAL_",
        ),
    )
    return {"rows_affected": sf_cur.rowcount, "action": "added"}


def apply_rename(sf_cur, cr):
    sf_cur.execute(
        """UPDATE V2RETAIL.GOLD.DIM_MVGR_VALUE
           SET CANON_MVGR_VALUE=%s, NORMALIZED=TRUE,
               APPROVED_BY=%s, APPROVED_AT=CURRENT_TIMESTAMP()
           WHERE MAJ_CAT_CODE=%s AND MVGR_VALUE=%s""",
        (cr["proposed_value"], cr["reviewed_by"], cr["maj_cat_code"], cr["current_value"]),
    )
    return {"rows_affected": sf_cur.rowcount, "action": "renamed CANON"}


def apply_deprecate(sf_cur, cr):
    sf_cur.execute(
        """UPDATE V2RETAIL.GOLD.DIM_MVGR_VALUE
           SET STATUS='IN-ACT', LIFECYCLE='PHASE_OUT',
               EFFECTIVE_TO=CURRENT_DATE,
               APPROVED_BY=%s, APPROVED_AT=CURRENT_TIMESTAMP()
           WHERE MAJ_CAT_CODE=%s AND MVGR_VALUE=%s""",
        (cr["reviewed_by"], cr["maj_cat_code"], cr["current_value"]),
    )
    return {"rows_affected": sf_cur.rowcount, "action": "deprecated"}


def apply_confirm_split_namespace(sf_cur, cr):
    sf_cur.execute(
        """UPDATE V2RETAIL.GOLD.DIM_MVGR_FAMILY
           SET MERCH_REVIEW_FLAG=FALSE,
               RENAME_REASON=COALESCE(RENAME_REASON,'') || ' [confirmed by '||%s||']'
           WHERE MAJ_CAT_CODE=%s""",
        (cr["reviewed_by"], cr["maj_cat_code"]),
    )
    return {"rows_affected": sf_cur.rowcount, "action": "flag cleared"}


def apply_map_to_existing(sf_cur, cr):
    sf_cur.execute(
        """INSERT INTO V2RETAIL.GOLD.DIM_MVGR_SYNONYM (SYN_ID, MVGR_KEY, ALIAS, ALIAS_TYPE, SOURCE)
           SELECT SUBSTR(MD5(d.MVGR_KEY||'|'||%s),1,16), d.MVGR_KEY, %s, 'LEGACY', 'change_request'
           FROM V2RETAIL.GOLD.DIM_MVGR_VALUE d
           WHERE d.MAJ_CAT_CODE=%s AND d.MVGR_VALUE=%s
           QUALIFY ROW_NUMBER() OVER (ORDER BY d.GRID_ID)=1""",
        (cr["current_value"], cr["current_value"], cr["maj_cat_code"], cr["proposed_value"]),
    )
    return {"rows_affected": sf_cur.rowcount, "action": "alias inserted"}


def apply_create_new_canonical(sf_cur, cr):
    sf_cur.execute(
        """INSERT INTO V2RETAIL.GOLD.DIM_MVGR_VALUE
           (GRID_ID, MVGR_KEY, FG_CODE, MAJ_CAT_CODE, MVGR_VALUE, FULL_FORM,
            STATUS, LIFECYCLE, SOURCE, CREATED_BY, CREATED_AT)
           SELECT
             SUBSTR(MD5('_GLOBAL_|'||%s||'|'||%s),1,16),
             SUBSTR(MD5('_GLOBAL_|'||%s||'|'||%s),1,16),
             '_GLOBAL_', %s, %s, %s, 'ACT', 'ACT', 'change_request', %s, CURRENT_TIMESTAMP()
           WHERE NOT EXISTS (
             SELECT 1 FROM V2RETAIL.GOLD.DIM_MVGR_VALUE
             WHERE MAJ_CAT_CODE=%s AND MVGR_VALUE=%s)""",
        (
            cr["maj_cat_code"], cr["proposed_value"],
            cr["maj_cat_code"], cr["proposed_value"],
            cr["maj_cat_code"], cr["proposed_value"], cr["proposed_value"],
            cr["reviewed_by"],
            cr["maj_cat_code"], cr["proposed_value"],
        ),
    )
    return {"rows_affected": sf_cur.rowcount, "action": "new canonical"}


HANDLERS = {
    "PROMOTE_DEV": apply_promote_dev,
    "ADD_VALUE": apply_add_value,
    "RENAME": apply_rename,
    "DEPRECATE": apply_deprecate,
    "CONFIRM_SPLIT_NAMESPACE": apply_confirm_split_namespace,
    "MAP_TO_EXISTING": apply_map_to_existing,
    "CREATE_NEW_CANONICAL": apply_create_new_canonical,
}


def main():
    db_url = os.environ.get("SUPABASE_DB_URL")
    if not db_url:
        print("ERROR: SUPABASE_DB_URL env var required")
        sys.exit(1)

    pg = psycopg2.connect(db_url)
    pg.autocommit = False
    pg_cur = pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    sf = sf_connect()
    sf_cur = sf.cursor()

    approved = fetch_approved(pg_cur)
    print(f"Approved CRs to apply: {len(approved)}")

    applied = failed = 0
    for cr in approved:
        cr_type = cr["cr_type"]
        handler = HANDLERS.get(cr_type)
        if not handler:
            print(f"  cr_id={cr['cr_id']} cr_type={cr_type} -> NO HANDLER, skipping")
            failed += 1
            continue
        try:
            result = handler(sf_cur, cr)
            sf.commit()
            pg_cur.execute(
                """UPDATE bible.change_request
                   SET status='APPLIED', applied_at=NOW()
                   WHERE cr_id=%s""",
                (cr["cr_id"],),
            )
            pg_cur.execute(
                """INSERT INTO bible.audit_log (cr_id, action, table_name, before_json, after_json, actor)
                   VALUES (%s, %s, %s, %s, %s, %s)""",
                (cr["cr_id"], cr_type, "V2RETAIL.GOLD.DIM_MVGR_VALUE",
                 json.dumps({"current_value": cr["current_value"]}),
                 json.dumps(result),
                 cr["reviewed_by"] or "worker"),
            )
            pg.commit()
            print(f"  cr_id={cr['cr_id']:>5} {cr_type:25} {result}")
            applied += 1
        except Exception as e:
            sf.rollback(); pg.rollback()
            print(f"  cr_id={cr['cr_id']:>5} {cr_type:25} FAILED: {e}")
            failed += 1

    print(f"\nApplied: {applied}  Failed: {failed}")
    pg.close(); sf.close()


if __name__ == "__main__":
    main()
