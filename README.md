# AEGIS – Regione Lazio Hydrological Data Client

Python helpers for programmatically fetching river flow velocity and water
level data from the AEGIS real-time monitoring system at
**https://temporeale.regione.lazio.it/aegis/**

---

## Reverse-engineered API

| Endpoint | Description |
|---|---|
| `POST /datascapeA/connect/token` | OAuth2 password grant → Bearer token |
| `GET /datascapeA/v1/elements[?category=]` | All elements with latest reading |
| `GET /datascapeA/v1/elements/<id>` | Single element metadata + latest value |
| `GET /datascapeA/v1/stations[?category=]` | All stations with alert state |
| `GET /datascapeA/v3/data-combo/<id>?from=&to=&…` | Historical time-series |

Auth uses public read-only credentials (`AegisPubblico` / `AegisPubblico`,
`client_id=Aegis`) that are embedded in the web app itself.

---

## Files

| File | Purpose |
|---|---|
| `client.py` | `AegisClient` class – auth, all API calls, pandas helper |
| `explore.py` | CLI to browse stations/elements |
| `fetch_data.py` | CLI to download time-series as CSV or JSON |

---

## Quick start

```bash
# List all water-level sensors
python explore.py --category level

# Search for "Tevere" river stations with velocity sensors
python explore.py --station "Tevere" --name "velocit"

# Get full metadata for element 49320 (surface velocity, Tevere a Foro Italico)
python explore.py --element-id 49320

# Fetch last 7 days of velocity + water level for Tevere a Foro Italico
python fetch_data.py --element-ids 49320 49322 --days 7 --out tevere.csv
```

## Example: use `AegisClient` in your own code

```python
from client import AegisClient

client = AegisClient()

# Latest readings for all river-level sensors
levels = client.list_elements(category="level")
for el in levels:
    print(el["stationName"], el["elementName"], el["value"], el["measUnit"])

# Time-series as a pandas DataFrame (last 3 days)
df = client.get_time_series_df(49320, days=3)
print(df.head())

# Find all elements at stations whose name contains "Tevere"
tevere_elements = client.find_elements(station_contains="Tevere")
```

---

## Categories

`all`, `rain`, `rain3Hours`, `rain6Hours`, `rain12Hours`, `rain24Hours`,
`rain48Hours`, `rain120Hours`, `level`, `airTemperature`,
`atmosphericPressure`, `wind`, `snowLevel`

---

## InfluxDB ingest (automated collection)

### 1. Install the extra dependency
```bash
python -m pip install influxdb-client
```

### 2. Configure `config.toml`
Edit the `[influxdb]` section with your connection details, and add one
`[[ingest.elements]]` block per sensor you want to track:
```toml
[influxdb]
url    = "http://localhost:8086"
token  = "YOUR_TOKEN"
org    = "your-org"
bucket = "aegis"

[[ingest.elements]]
id    = 49320
label = "Tevere a Foro Italico – Surface Velocity"

[[ingest.elements]]
id    = 49322
label = "Tevere a Foro Italico – Water Level"
```

### 3. Test without writing anything
```bash
python ingest.py --dry-run -v
```

### 4. Run once to fill history
```bash
python ingest.py
```

### 5. Schedule periodic runs

**Windows Task Scheduler** (every 15 minutes):
```
Program : C:\Users\...\Python\bin\python3.14.exe
Arguments: "G:\Programming\Python projects\AEGIS\ingest.py"
Start in : G:\Programming\Python projects\AEGIS
```

**Linux/macOS cron** (every 15 minutes):
```cron
*/15 * * * * /usr/bin/python3 /path/to/AEGIS/ingest.py >> /var/log/aegis_ingest.log 2>&1
```

### InfluxDB data model

| Field | Value |
|---|---|
| measurement | `river_monitoring` |
| tag `element_id` | AEGIS element ID (e.g. `49320`) |
| tag `station` | Station name (e.g. `Tevere a Foro Italico`) |
| tag `element_name` | Sensor name (e.g. `Velocita' Superficiale`) |
| tag `unit` | Unit of measure (e.g. `m/s`, `m`) |
| field `value` | Numeric reading (float) |
| timestamp | UTC, second precision |

### State tracking
`ingest.py` writes `.ingest_state.json` after each successful run, storing
the latest timestamp per element.  The next run starts from there (minus a
configurable overlap window), so no data is missed and duplicates are handled
via InfluxDB's idempotent write semantics.

---

## Notable element IDs (Tevere a Foro Italico)

| ID | Name | Unit |
|---|---|---|
| `49320` | Surface Water Velocity | m/s |
| `49322` | Water Level | m |
