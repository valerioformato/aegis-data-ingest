"""
ingest.py – One-shot AEGIS → InfluxDB 2.x ingestion script.

Designed to be called periodically by Windows Task Scheduler, cron, or any
other scheduler.  On each invocation it:

  1. Reads config.toml (or a path given with --config).
  2. Loads .ingest_state.json to find the last-fetched timestamp per element.
  3. Authenticates with the AEGIS API.
  4. For each configured element, fetches data from (last_fetched - overlap)
     to now, then writes all points to InfluxDB.
  5. Updates the state file so the next run starts from where this one left off.

InfluxDB data model
───────────────────
  Measurements are grouped by physical domain:

  river_monitoring  – Water Level, Flow Rate, Surface Velocity,
                      Idrometro Mignone/Verginese, Portata Rlazio
  precipitation     – Accumulated Rainfall, Rainfall Intensity
  weather           – Air Temperature, Relative Humidity,
                      Atmospheric Pressure, Direct Solar Radiation
  wind              – Vector/Scalar Wind Speed & Direction
  snow              – Snow Level

  All share the same tag/field schema:
  tags  : element_id, station, element_name, unit
  field : value  (float)
  time  : UTC, second precision

Usage
─────
  python ingest.py                        # uses config.toml in same directory
  python ingest.py --config /path/to/config.toml
  python ingest.py --dry-run              # fetch but don't write to InfluxDB
  python ingest.py -v                     # verbose logging
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import tomllib  # stdlib since Python 3.11

from client import AegisClient
import state as state_store
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# Maps AEGIS elementName → InfluxDB measurement name.
# The comparison is case-insensitive; unrecognised names fall back to "aegis_other".
_MEASUREMENT_MAP: dict[str, str] = {
    # ── River / hydrology ──────────────────────────────────────────────────────
    "water level":              "river_monitoring",
    "flow rate":                "river_monitoring",
    "velocita' superficiale":   "river_monitoring",
    "surface water velocity":   "river_monitoring",
    "idrometro mignone":        "river_monitoring",
    "idrometro verginese":      "river_monitoring",
    "portata rlazio":           "river_monitoring",
    # ── Precipitation ──────────────────────────────────────────────────────────
    "accumulated rainfall":     "precipitation",
    "rainfall intensity":       "precipitation",
    # ── Atmosphere / weather ───────────────────────────────────────────────────
    "air temperature":          "weather",
    "relative humidity":        "weather",
    "atmospheric pressure":     "weather",
    "direct solar radiation":   "weather",
    # ── Wind ───────────────────────────────────────────────────────────────────
    "vector wind speed":        "wind",
    "vector wind direction":    "wind",
    "scalar wind speed":        "wind",
    "scalar wind direction":    "wind",
    # ── Snow ───────────────────────────────────────────────────────────────────
    "snow level":               "snow",
}

_FALLBACK_MEASUREMENT = "aegis_other"


def _measurement_for(element_name: str) -> str:
    return _MEASUREMENT_MAP.get(element_name.lower(), _FALLBACK_MEASUREMENT)

log = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Config loading
# ──────────────────────────────────────────────────────────────────────────────

def load_config(path: Path) -> dict:
    with open(path, "rb") as f:
        return tomllib.load(f)


# ──────────────────────────────────────────────────────────────────────────────
# Core logic
# ──────────────────────────────────────────────────────────────────────────────

def run(config: dict, dry_run: bool = False) -> None:
    ingest_cfg = config["ingest"]
    influx_cfg = config["influxdb"]
    elements   = ingest_cfg["elements"]

    initial_lookback = timedelta(hours=ingest_cfg.get("initial_lookback_hours", 24))
    overlap          = timedelta(minutes=ingest_cfg.get("overlap_minutes", 30))
    now              = datetime.now(timezone.utc)

    # ── Load state ────────────────────────────────────────────────────────────
    saved_state = state_store.load()
    log.info("Loaded state for %d element(s).", len(saved_state))

    # ── AEGIS client ──────────────────────────────────────────────────────────
    aegis = AegisClient()

    # ── InfluxDB client ───────────────────────────────────────────────────────
    if not dry_run:
        influx = InfluxDBClient(
            url=influx_cfg["url"],
            token=influx_cfg["token"],
            org=influx_cfg["org"],
        )
        write_api = influx.write_api(write_options=SYNCHRONOUS)
    else:
        influx = None
        write_api = None
        log.info("DRY-RUN mode – nothing will be written to InfluxDB.")

    new_state = dict(saved_state)
    total_points = 0

    for elem_cfg in elements:
        eid   = int(elem_cfg["id"])
        label = elem_cfg.get("label", str(eid))

        # Determine fetch window
        if eid in saved_state:
            fetch_from = saved_state[eid] - overlap
        else:
            fetch_from = now - initial_lookback
            log.info("[%s] No prior state – fetching last %.1f h.",
                     label, initial_lookback.total_seconds() / 3600)

        log.info("[%s] Fetching from %s to %s …",
                 label, fetch_from.isoformat(), now.isoformat())

        try:
            raw = aegis.get_time_series(eid, from_dt=fetch_from, to_dt=now)
        except Exception as exc:
            log.error("[%s] AEGIS fetch failed: %s", label, exc)
            continue

        detail       = raw.get("elementDetail", {})
        station_name = detail.get("stationName", "")
        element_name = detail.get("elementName", "")
        unit         = detail.get("measUnit", "")

        rows = raw.get("plausibleData", []) + raw.get("extempData", [])
        if not rows:
            log.warning("[%s] No data returned.", label)
            continue

        log.info("[%s] %d point(s) received (%s – %s, unit=%s) → measurement: %s.",
                 label, len(rows), station_name, element_name, unit,
                 _measurement_for(element_name))

        # Build InfluxDB points
        points: list[Point] = []
        latest_ts: datetime | None = None

        for epoch_ms, value in rows:
            if value is None:
                continue
            ts = datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc)
            measurement = _measurement_for(element_name)
            p = (
                Point(measurement)
                .tag("element_id",   str(eid))
                .tag("station",      station_name)
                .tag("element_name", element_name)
                .tag("unit",         unit)
                .field("value",      float(value))
                .time(ts, WritePrecision.S)
            )
            points.append(p)
            if latest_ts is None or ts > latest_ts:
                latest_ts = ts

        if not points:
            log.warning("[%s] All values were None, skipping.", label)
            continue

        # Write to InfluxDB
        if not dry_run:
            try:
                write_api.write(
                    bucket=influx_cfg["bucket"],
                    org=influx_cfg["org"],
                    record=points,
                )
                log.info("[%s] ✓ Wrote %d point(s) to InfluxDB.", label, len(points))
            except Exception as exc:
                log.error("[%s] InfluxDB write failed: %s", label, exc)
                continue
        else:
            # In dry-run, print the first and last point for inspection
            log.info("[%s] DRY-RUN: would write %d point(s).", label, len(points))
            log.debug("  First: %s", points[0].to_line_protocol())
            log.debug("  Last:  %s", points[-1].to_line_protocol())

        total_points += len(points)
        if latest_ts:
            new_state[eid] = latest_ts

    # ── Persist updated state ─────────────────────────────────────────────────
    if not dry_run and new_state != saved_state:
        state_store.save(new_state)
        log.info("State updated for %d element(s).", len(new_state))

    log.info("Done. Total points written: %d.", total_points)

    if influx:
        influx.close()


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest AEGIS data into InfluxDB 2.x.")
    parser.add_argument(
        "--config", type=Path,
        default=Path(__file__).parent / "config.toml",
        help="Path to config.toml (default: config.toml next to this script).",
    )
    parser.add_argument(
        "--state", type=Path,
        default=state_store.DEFAULT_PATH,
        help="Path to the state JSON file (default: .ingest_state.json).",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Fetch data from AEGIS but do not write to InfluxDB.",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true",
        help="Enable DEBUG logging.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )

    state_store.DEFAULT_PATH = args.state

    if not args.config.exists():
        log.error("Config file not found: %s", args.config)
        sys.exit(1)

    config = load_config(args.config)
    run(config, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
