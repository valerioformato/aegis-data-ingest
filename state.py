"""
state.py – Persist the last-fetched timestamp per element ID.

The state file is a simple JSON file (default: .ingest_state.json) that maps
element IDs to ISO-8601 timestamps.  On each run ingest.py reads it to know
where to start fetching, and writes back after a successful InfluxDB flush.

Example state file content:
  {
    "49320": "2026-03-13T09:00:00+00:00",
    "49322": "2026-03-13T09:00:00+00:00"
  }
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)

DEFAULT_PATH = Path(__file__).parent / ".ingest_state.json"


def load(path: Path = DEFAULT_PATH) -> dict[int, datetime]:
    """Return {element_id: last_fetched_datetime} from the state file."""
    if not path.exists():
        return {}
    try:
        raw: dict[str, str] = json.loads(path.read_text(encoding="utf-8"))
        return {
            int(k): datetime.fromisoformat(v)
            for k, v in raw.items()
        }
    except Exception as exc:
        log.warning("Could not read state file %s: %s – starting fresh.", path, exc)
        return {}


def save(state: dict[int, datetime], path: Path = DEFAULT_PATH) -> None:
    """Persist {element_id: datetime} to the state file."""
    serialisable = {str(k): v.isoformat() for k, v in state.items()}
    path.write_text(json.dumps(serialisable, indent=2), encoding="utf-8")
    log.debug("State saved to %s", path)
