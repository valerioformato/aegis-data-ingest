"""
explore.py – Browse AEGIS stations and elements interactively.

Usage examples:

  # List all water-level elements
  python explore.py --category level

  # Search for elements at a specific station
  python explore.py --station "Tevere"

  # Show details for a specific element ID
  python explore.py --element-id 49320

  # Search for "velocity" elements
  python explore.py --name "velocit"
"""

import argparse
import json
import sys

from client import AegisClient, CATEGORIES


def main() -> None:
    parser = argparse.ArgumentParser(description="Explore AEGIS monitoring stations and elements.")
    parser.add_argument("--category", choices=CATEGORIES, default="all",
                        help="Filter by measurement category (default: all).")
    parser.add_argument("--station", default="",
                        help="Case-insensitive substring filter on station name.")
    parser.add_argument("--name", default="",
                        help="Case-insensitive substring filter on element name.")
    parser.add_argument("--element-id", type=int,
                        help="Show full metadata for a single element ID.")
    parser.add_argument("--json", action="store_true",
                        help="Output raw JSON instead of a table.")
    args = parser.parse_args()

    client = AegisClient()

    if args.element_id:
        data = client.get_element(args.element_id)
        if args.json:
            print(json.dumps(data, indent=2, ensure_ascii=False))
        else:
            _print_element_detail(data)
        return

    elements = client.find_elements(
        name_contains=args.name,
        station_contains=args.station,
        category=args.category,
    )

    if not elements:
        print("No elements found matching the given filters.", file=sys.stderr)
        sys.exit(1)

    if args.json:
        print(json.dumps(elements, indent=2, ensure_ascii=False))
        return

    _print_table(elements)


def _print_table(elements: list[dict]) -> None:
    header = f"{'ID':>8}  {'Station':<35}  {'Element':<30}  {'Value':>10}  {'Unit':<8}  {'Time':<22}  State"
    print(header)
    print("-" * len(header))
    for e in elements:
        val = e.get("value")
        val_str = f"{val:.{e.get('decimals', 2)}f}" if val is not None else "N/A"
        t = (e.get("time") or "")[:19]
        state = e.get("stateId", "")
        print(
            f"{e['elementId']:>8}  "
            f"{e.get('stationName', ''):<35.35}  "
            f"{e.get('elementName', ''):<30.30}  "
            f"{val_str:>10}  "
            f"{e.get('measUnit', ''):<8}  "
            f"{t:<22}  "
            f"{state}"
        )
    print(f"\nTotal: {len(elements)} element(s)")


def _print_element_detail(e: dict) -> None:
    fields = [
        ("Element ID",      e.get("elementId")),
        ("Element Name",    e.get("elementName")),
        ("Station ID",      e.get("stationId")),
        ("Station Name",    e.get("stationName")),
        ("Quantity",        e.get("quantity")),
        ("Measurement Unit",e.get("measUnit")),
        ("Instrument",      e.get("instrument")),
        ("Current Value",   f"{e.get('value')} {e.get('measUnit')} @ {e.get('time')}"),
        ("Trend",           e.get("trend")),
        ("State ID",        e.get("stateId")),
    ]
    for label, value in fields:
        print(f"  {label:<20}: {value}")


if __name__ == "__main__":
    main()
