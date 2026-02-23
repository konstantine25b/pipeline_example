import argparse
import csv
import time
from datetime import datetime
from pathlib import Path

import requests


def fetch_nbg_currencies(url: str):
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()
    if not data:
        return []
    entry = data[0]
    as_of_date = entry.get("date")
    currencies = entry.get("currencies", [])
    records = []
    fetched_at = datetime.utcnow().isoformat()
    for c in currencies:
        record = {
            "fetched_at": fetched_at,
            "as_of_date": as_of_date,
            "code": c.get("code"),
            "quantity": c.get("quantity"),
            "rate": c.get("rate"),
            "rateFormated": c.get("rateFormated"),
            "diff": c.get("diff"),
            "diffFormated": c.get("diffFormated"),
            "name": c.get("name"),
            "validFromDate": c.get("validFromDate"),
        }
        records.append(record)
    return records


def ensure_csv_header(csv_path: Path, fieldnames):
    if not csv_path.exists():
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()


def append_records_to_csv(csv_path: Path, records):
    if not records:
        return
    fieldnames = list(records[0].keys())
    ensure_csv_header(csv_path, fieldnames)
    with csv_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writerows(records)


def run_once(url: str, csv_path: Path):
    records = fetch_nbg_currencies(url)
    append_records_to_csv(csv_path, records)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="https://nbg.gov.ge/gw/api/ct/monetarypolicy/currencies/en/json")
    parser.add_argument("--interval-minutes", type=float, default=5.0)
    parser.add_argument("--csv-path", type=str, default="data/nbg_currencies.csv")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    csv_path = Path(args.csv_path)
    if not csv_path.parent.exists():
        csv_path.parent.mkdir(parents=True, exist_ok=True)
    if args.once:
        run_once(args.url, csv_path)
        return
    while True:
        run_once(args.url, csv_path)
        time.sleep(args.interval_minutes * 60)


if __name__ == "__main__":
    main()

