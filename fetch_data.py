"""
fetch_data.py – Download time-series measurements for one or more elements.

Usage examples:

  # Fetch last 24 h for element 49320 (surface velocity, Tevere a Foro Italico)
  python fetch_data.py --element-ids 49320

  # Fetch both velocity and water level for the same station, last 7 days
  python fetch_data.py --element-ids 49320 49322 --days 7

  # Specify an explicit date range
  python fetch_data.py --element-ids 49320 --from 2026-03-01 --to 2026-03-13

  # Save to CSV
  python fetch_data.py --element-ids 49320 49322 --days 3 --out data.csv

  # Print as JSON
  python fetch_data.py --element-ids 49320 --json
"""

import argparse
import json
import sys
from datetime import datetime, timezone

from client import AegisClient


def parse_date(s: str) -> datetime:
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(f"Cannot parse date: {s!r}  (expected YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch AEGIS time-series data.")
    parser.add_argument("--element-ids", nargs="+", type=int, required=True,
                        help="One or more AEGIS element IDs.")
    parser.add_argument("--days", type=int, default=1,
                        help="Number of past days to fetch (default: 1). Ignored if --from/--to are set.")
    parser.add_argument("--from", dest="from_dt", type=parse_date, default=None,
                        help="Start datetime, e.g. 2026-03-01 or 2026-03-01T08:00:00 (UTC).")
    parser.add_argument("--to", dest="to_dt", type=parse_date, default=None,
                        help="End datetime (UTC). Defaults to now.")
    parser.add_argument("--out", default=None,
                        help="Output CSV file path. Prints to stdout if omitted.")
    parser.add_argument("--json", action="store_true",
                        help="Output raw JSON instead of CSV.")
    args = parser.parse_args()

    client = AegisClient()

    if args.json:
        results = {}
        for eid in args.element_ids:
            print(f"Fetching element {eid}…", file=sys.stderr)
            raw = client.get_time_series(
                eid,
                from_dt=args.from_dt,
                to_dt=args.to_dt,
                days=args.days,
            )
            results[eid] = raw
        print(json.dumps(results, indent=2, ensure_ascii=False))
        return

    # CSV output (pandas)
    import pandas as pd

    frames = []
    for eid in args.element_ids:
        print(f"Fetching element {eid}…", file=sys.stderr)
        raw = client.get_time_series(
            eid,
            from_dt=args.from_dt,
            to_dt=args.to_dt,
            days=args.days,
        )
        detail = raw.get("elementDetail", {})
        station = detail.get("stationName", "")
        element = detail.get("elementName", "")
        unit = detail.get("measUnit", "")

        rows = raw.get("plausibleData", []) + raw.get("extempData", [])
        if not rows:
            print(f"  ⚠ No data for element {eid}", file=sys.stderr)
            continue

        df = pd.DataFrame(rows, columns=["epoch_ms", "value"])
        df["time"] = pd.to_datetime(df["epoch_ms"], unit="ms", utc=True)
        df["element_id"] = eid
        df["station"] = station
        df["element"] = element
        df["unit"] = unit
        df = df[["time", "element_id", "station", "element", "value", "unit"]]
        df = df.sort_values("time").reset_index(drop=True)
        frames.append(df)
        print(f"  ✓ {len(df)} samples  [{station} – {element} ({unit})]", file=sys.stderr)

    if not frames:
        print("No data retrieved.", file=sys.stderr)
        sys.exit(1)

    combined = pd.concat(frames, ignore_index=True)

    if args.out:
        combined.to_csv(args.out, index=False)
        print(f"\nSaved {len(combined)} rows to {args.out}", file=sys.stderr)
    else:
        print(combined.to_csv(index=False))


if __name__ == "__main__":
    main()
