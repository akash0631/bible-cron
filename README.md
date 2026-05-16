# bible-cron

Scheduled workers for the V2 Retail Master-Data Bible.

## What it does

- **Daily sync** (06:00 UTC = 11:30 IST): `sync_bible_to_supabase.py` — pull `V2RETAIL.GOLD.DIM_MVGR_*` from Snowflake into Supabase v2srm `bible.*` schema. Truncate+COPY pattern.
- **Every 15 min**: `apply_bible_change_requests.py` — consume `bible.change_request` rows with `status='APPROVED'`, apply matching mutation to Snowflake `V2RETAIL.GOLD.DIM_MVGR_*`, write `bible.audit_log` entry. Status flips to `APPLIED`.

## Required secrets (GH repo Settings -> Secrets and variables -> Actions)

- `SF_PRIVATE_KEY_B64` — `base64 -w0 ~/.snowflake/akashv2kart_rsa.p8`
- `SUPABASE_V2SRM_DB_URL` — `postgresql://postgres.pymdqnnwwxrgeolvgvgv:<pw>@aws-0-ap-south-1.pooler.supabase.com:5432/postgres`

## Manual run

Actions tab -> "Bible - Sync + Apply Approvals" -> Run workflow -> pick mode (sync / apply / both).

## Local dev

```bash
SUPABASE_DB_URL='postgresql://...' python scripts/sync_bible_to_supabase.py
SUPABASE_DB_URL='postgresql://...' python scripts/apply_bible_change_requests.py
```

Needs Snowflake keypair at `~/.snowflake/akashv2kart_rsa.p8`.

## Related

- Snowflake source: `V2RETAIL.GOLD.DIM_MVGR_*`
- Supabase target: project `pymdqnnwwxrgeolvgvgv`, schema `bible.*`
- Frontend: `akash0631/po-wise-wardrobe` (`/bible/*` routes)
- Wiki: `[[Master Data Bible 2026-05-15]]`
