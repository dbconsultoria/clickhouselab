import gzip
import tempfile
from datetime import datetime
from pathlib import Path

import clickhouse_connect

LOCAL_PATH = Path(tempfile.gettempdir()) / "trips_0.gz"

_COLUMNS = [
    "trip_id", "pickup_datetime", "dropoff_datetime",
    "pickup_longitude", "pickup_latitude",
    "dropoff_longitude", "dropoff_latitude",
    "passenger_count", "trip_distance",
    "fare_amount", "tip_amount", "total_amount",
    "payment_type", "pickup_ntaname", "dropoff_ntaname",
]

# Read first 5 data rows
rows = []
with gzip.open(LOCAL_PATH, "rt", encoding="utf-8", errors="replace") as gz:
    header = [c.strip() for c in gz.readline().split("\t")]
    print("HEADER:", header)
    print()
    for i, line in enumerate(gz):
        if i >= 5:
            break
        fields = line.rstrip("\n").split("\t")
        raw = dict(zip(header, fields))
        print(f"ROW {i}:", {k: raw.get(k) for k in _COLUMNS})
    print()

# Re-read and build typed rows
print("=== Testing type coercion ===")
rows = []
with gzip.open(LOCAL_PATH, "rt", encoding="utf-8", errors="replace") as gz:
    header = [c.strip() for c in gz.readline().split("\t")]
    for i, line in enumerate(gz):
        if i >= 5:
            break
        fields = line.rstrip("\n").split("\t")
        raw = dict(zip(header, fields))

        def coerce(col, val):
            val = val.strip()
            try:
                if col in {"trip_id"}:
                    return int(val) if val else 0
                if col in {"passenger_count"}:
                    return int(float(val)) if val else 0
                if col in {"pickup_longitude","pickup_latitude","dropoff_longitude",
                           "dropoff_latitude","trip_distance","fare_amount",
                           "tip_amount","total_amount"}:
                    return float(val) if val else 0.0
                if col in {"pickup_datetime","dropoff_datetime"}:
                    return datetime.strptime(val, "%Y-%m-%d %H:%M:%S") if val else datetime(1970,1,1)
            except Exception as e:
                print(f"  COERCE ERROR col={col} val={repr(val)}: {e}")
                return None
            return val

        row = [coerce(col, raw.get(col, "")) for col in _COLUMNS]
        print(f"ROW {i} typed:", row)
        rows.append(row)

print()
print("=== Inserting 5 rows ===")
client = clickhouse_connect.get_client(host="localhost", port=8123, username="default", password="")
client.command("CREATE DATABASE IF NOT EXISTS nyc_taxi")
client.command("""
    CREATE TABLE IF NOT EXISTS nyc_taxi.trips (
        trip_id             UInt32,
        pickup_datetime     DateTime,
        dropoff_datetime    DateTime,
        pickup_longitude    Float32,
        pickup_latitude     Float32,
        dropoff_longitude   Float32,
        dropoff_latitude    Float32,
        passenger_count     UInt8,
        trip_distance       Float32,
        fare_amount         Float32,
        tip_amount          Float32,
        total_amount        Float32,
        payment_type        LowCardinality(String),
        pickup_ntaname      LowCardinality(String),
        dropoff_ntaname     LowCardinality(String)
    ) ENGINE = MergeTree()
    ORDER BY (pickup_datetime, trip_id)
""")

try:
    client.insert("nyc_taxi.trips", rows, column_names=_COLUMNS)
    print("INSERT OK")
    result = client.query("SELECT count() FROM nyc_taxi.trips")
    print("COUNT:", result.result_rows)
except Exception as e:
    print("INSERT ERROR:", e)
