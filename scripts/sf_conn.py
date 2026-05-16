"""Shared Snowflake connection helper for V2 MRST pipeline.

Uses key-pair auth against the PROGRAMMER_SVC service user.
Back-dates the JWT iat by 5 minutes to survive local/server clock skew
(Windows clock runs ~4 min fast on this machine).

Usage:
    from sf_conn import connect
    sf = connect()
    cur = sf.cursor()
    cur.execute("SELECT ...")
"""
import os
import datetime
import snowflake.connector
from snowflake.connector.auth import keypair as _kp_mod

_CLOCK_SKEW_MIN = int(os.environ.get("SF_CLOCK_SKEW_MIN", "2"))  # back-date iat to compensate for local clock drift; was 5 in 2026-04, dropped to 2 on 2026-05-02 after Windows clock got more accurate (now ~2 min fast vs server)

# Monkey-patch AuthByKeyPair.prepare to back-date iat
_orig_prepare = _kp_mod.AuthByKeyPair.prepare
_real_dt = _kp_mod.datetime


class _ShiftedDT:
    @staticmethod
    def now(tz=None):
        return _real_dt.now(tz) - datetime.timedelta(minutes=_CLOCK_SKEW_MIN)

    @staticmethod
    def fromtimestamp(ts, tz=None):
        return _real_dt.fromtimestamp(ts, tz)


def _patched_prepare(self, *, account, user, **kwargs):
    _kp_mod.datetime = _ShiftedDT
    try:
        return _orig_prepare(self, account=account, user=user, **kwargs)
    finally:
        _kp_mod.datetime = _real_dt


_kp_mod.AuthByKeyPair.prepare = _patched_prepare


def connect(role="PROGRAMMER_DEV_ROLE", warehouse="COMPUTE_WH",
            database="V2RETAIL", schema="GOLD"):
    return snowflake.connector.connect(
        account="iafphkw-hh80816",
        user="PROGRAMMER_SVC",
        authenticator="SNOWFLAKE_JWT",
        private_key_file=os.path.expanduser("~/.snowflake/akashv2kart_rsa.p8"),
        role=role,
        warehouse=warehouse,
        database=database,
        schema=schema,
        insecure_mode=True,
    )


if __name__ == "__main__":
    sf = connect()
    cur = sf.cursor()
    cur.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_ACCOUNT()")
    print("OK:", cur.fetchone())
