#!/usr/bin/env python3
import argparse
import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text


DEFAULT_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


def build_engine(user: str, password: str, host: str, port: int, db: str):
    uri = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    return create_engine(uri)


def parse_args():
    p = argparse.ArgumentParser(description="Ingest NYC taxi zone lookup into Postgres")
    p.add_argument("--url", default=DEFAULT_URL, help="CSV URL for taxi_zone_lookup")
    p.add_argument("--table", default="taxi_zones", help="Target table name")
    p.add_argument("--if-exists", default="replace", choices=["replace", "append", "fail"])
    p.add_argument("--create-pk", action="store_true", help="Create PK on LocationID after load")
    p.add_argument("--pg-user", default=os.getenv("PGUSER", "root"))
    p.add_argument("--pg-pass", default=os.getenv("PGPASSWORD", "root"))
    p.add_argument("--pg-host", default=os.getenv("PGHOST", "localhost"))
    p.add_argument("--pg-port", type=int, default=int(os.getenv("PGPORT", "5432")))
    p.add_argument("--pg-db", default=os.getenv("PGDATABASE", "ny_taxi"))
    return p.parse_args()


def main():
    args = parse_args()
    engine = build_engine(args.pg_user, args.pg_pass, args.pg_host, args.pg_port, args.pg_db)

    print(f"Downloading zones CSV: {args.url}")
    df = pd.read_csv(args.url)

    # Clean + enforce types
    expected_cols = ["LocationID", "Borough", "Zone", "service_zone"]
    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns: {missing}. Found: {list(df.columns)}")

    df = df[expected_cols].copy()
    df["LocationID"] = pd.to_numeric(df["LocationID"], errors="raise").astype("int64")
    df["Borough"] = df["Borough"].astype("string").str.strip()
    df["Zone"] = df["Zone"].astype("string").str.strip()
    df["service_zone"] = df["service_zone"].astype("string").str.strip()

    # Write to Postgres
    print(f"Loading {len(df)} rows into {args.pg_db}.{args.table} (if_exists={args.if_exists})")
    df.to_sql(name=args.table, con=engine, if_exists=args.if_exists, index=False, method="multi")

    # Optional constraints/indexes
    if args.create_pk:
        # If replace was used, constraints are gone; we add them back safely.
        with engine.begin() as conn:
            conn.execute(text(f'ALTER TABLE "{args.table}" DROP CONSTRAINT IF EXISTS "{args.table}_pkey";'))
            conn.execute(text(f'ALTER TABLE "{args.table}" ADD PRIMARY KEY ("LocationID");'))
        print("Primary key created on LocationID.")

    # Verify
    with engine.connect() as conn:
        count = conn.execute(text(f'SELECT COUNT(*) FROM "{args.table}";')).scalar_one()
        sample = conn.execute(text(f'SELECT * FROM "{args.table}" ORDER BY "LocationID" LIMIT 5;')).fetchall()

    print(f"✅ Total zones loaded: {count}")
    print("✅ First 5 rows:")
    for row in sample:
        print(row)

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        raise