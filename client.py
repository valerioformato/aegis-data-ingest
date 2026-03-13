"""
AEGIS API Client for Regione Lazio hydrological monitoring system.

Reverse-engineered from https://temporeale.regione.lazio.it/aegis/

Auth flow:
  POST /datascapeA/connect/token  (password grant, client_id="Aegis")
  → Bearer token, expires in 86400s

Key endpoints (all under https://temporeale.regione.lazio.it/datascapeA/):
  GET /v1/elements                           → all elements (latest reading)
  GET /v1/elements?category=<cat>            → filter by category
  GET /v1/elements/<elementId>               → single element detail
  GET /v1/stations?category=<cat>            → station list
  GET /v3/data-combo/<elementId>             → time-series data for one element
  GET /v3/elements/<elementId>               → extended element metadata
"""

from __future__ import annotations

import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

BASE_URL = "https://temporeale.regione.lazio.it"
TOKEN_URL = f"{BASE_URL}/datascapeA/connect/token"
API_BASE = f"{BASE_URL}/datascapeA"

# Public read-only credentials (hardcoded in the AEGIS web app itself)
_USERNAME = "AegisPubblico"
_PASSWORD = "AegisPubblico"
_CLIENT_ID = "Aegis"

# Valid category values (from /v1/aegis/configuration)
CATEGORIES = [
    "all", "rain", "rain3Hours", "rain6Hours", "rain12Hours",
    "rain24Hours", "rain48Hours", "rain120Hours",
    "level", "airTemperature", "atmosphericPressure", "wind", "snowLevel",
]


class AegisClient:
    """Thin wrapper around the AEGIS REST API."""

    def __init__(self) -> None:
        self._session = requests.Session()
        self._token: str | None = None
        self._token_expires_at: float = 0.0
        self._client_instance = str(uuid.uuid4())

    # ------------------------------------------------------------------ auth

    def _ensure_token(self) -> None:
        """Authenticate (or re-authenticate) if the token is missing/expired."""
        if self._token and time.time() < self._token_expires_at - 60:
            return

        resp = requests.post(
            TOKEN_URL,
            data={
                "username": _USERNAME,
                "password": _PASSWORD,
                "grant_type": "password",
                "client_id": _CLIENT_ID,
                "client_instance": self._client_instance,
            },
        )
        resp.raise_for_status()
        data = resp.json()
        self._token = data["access_token"]
        self._token_expires_at = time.time() + data.get("expires_in", 86400)

    def _get(self, path: str, params: dict | None = None) -> Any:
        self._ensure_token()
        url = f"{API_BASE}{path}"
        resp = self._session.get(
            url,
            params=params,
            headers={"Authorization": f"Bearer {self._token}"},
        )
        resp.raise_for_status()
        return resp.json()

    # --------------------------------------------------------------- elements

    def list_elements(self, category: str = "all") -> list[dict]:
        """
        Return all elements with their latest reading.

        Each item contains: elementId, stationName, stationId, elementName,
        value, measUnit, trend, time, stateId, decimals.

        Args:
            category: one of CATEGORIES.  Default "all".
        """
        return self._get("/v1/elements", {"category": category})

    def get_element(self, element_id: int) -> dict:
        """Return metadata + latest value for a single element."""
        return self._get(f"/v1/elements/{element_id}")

    # --------------------------------------------------------------- stations

    def list_stations(self, category: str = "all") -> list[dict]:
        """
        Return all stations (with alert state and latest timestamp).

        Each item contains: stationId, stationName, time, stateId.

        Args:
            category: one of CATEGORIES.  Default "all".
        """
        return self._get("/v1/stations", {"category": category})

    # ---------------------------------------------------------- time-series

    def get_time_series(
        self,
        element_id: int,
        from_dt: datetime | None = None,
        to_dt: datetime | None = None,
        days: int = 1,
        timing: str = "SmartEquispaced",
        basic_type: str = "Plausible",
    ) -> dict:
        """
        Fetch time-series data for *element_id*.

        Returns a dict with keys:
          - "elementDetail": element metadata
          - "plausibleData":  list of [epoch_ms, value] pairs
          - "extempData":     list of [epoch_ms, value] pairs (real-time extras)

        Args:
            element_id:  AEGIS element ID (e.g. 49320).
            from_dt:     Start of period.  Defaults to ``days`` ago.
            to_dt:       End of period.  Defaults to now.
            days:        Convenience shortcut when from_dt/to_dt are None.
            timing:      "SmartEquispaced" (default) or "ReducedEquispaced".
            basic_type:  "Plausible" (default) or "Auto".
        """
        if to_dt is None:
            to_dt = datetime.now(timezone.utc)
        if from_dt is None:
            from_dt = to_dt - timedelta(days=days)

        params = {
            "from": from_dt.isoformat(),
            "to": to_dt.isoformat(),
            "basicType": basic_type,
            "part": ["EpochTime", "ValueWithInvalid"],
            "timing": timing,
            "loadAlsoExtemp": "true",
            "ui_culture": "en",
        }
        return self._get(f"/v3/data-combo/{element_id}", params)

    def get_time_series_df(
        self,
        element_id: int,
        *,
        from_dt: datetime | None = None,
        to_dt: datetime | None = None,
        days: int = 1,
    ):
        """
        Same as ``get_time_series`` but returns a pandas DataFrame with
        columns ["time", "value"] indexed by UTC datetime.

        Requires pandas.
        """
        import pandas as pd

        raw = self.get_time_series(element_id, from_dt=from_dt, to_dt=to_dt, days=days)
        rows = raw.get("plausibleData", []) + raw.get("extempData", [])
        if not rows:
            return pd.DataFrame(columns=["time", "value"])

        df = pd.DataFrame(rows, columns=["epoch_ms", "value"])
        df["time"] = pd.to_datetime(df["epoch_ms"], unit="ms", utc=True)
        df = df[["time", "value"]].sort_values("time").reset_index(drop=True)
        return df

    # ----------------------------------------------------- convenience search

    def find_elements(
        self,
        name_contains: str = "",
        station_contains: str = "",
        category: str = "all",
    ) -> list[dict]:
        """
        Filter elements by (case-insensitive) substring in name or station name.

        Args:
            name_contains:    Substring to search in elementName.
            station_contains: Substring to search in stationName.
            category:         Category filter passed to list_elements.
        """
        elements = self.list_elements(category=category)
        nc = name_contains.lower()
        sc = station_contains.lower()
        return [
            e for e in elements
            if nc in e.get("elementName", "").lower()
            and sc in e.get("stationName", "").lower()
        ]
