import csv
import io
import os
from datetime import datetime
from pathlib import Path

import requests
from flask import Flask, Response, jsonify, request


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


app = Flask(__name__)


@app.get("/currencies")
def currencies():
    url = request.args.get("url", default="https://nbg.gov.ge/gw/api/ct/monetarypolicy/currencies/en/json")
    csv_path_str = request.args.get("csv_path", default="data/nbg_currencies.csv")
    csv_path = Path(csv_path_str)
    if not csv_path.parent.exists():
        csv_path.parent.mkdir(parents=True, exist_ok=True)
    records = fetch_nbg_currencies(url)
    append_records_to_csv(csv_path, records)
    return jsonify(records)


@app.get("/currencies.csv")
def currencies_csv():
    url = request.args.get("url", default="https://nbg.gov.ge/gw/api/ct/monetarypolicy/currencies/en/json")
    csv_path_str = request.args.get("csv_path", default="data/nbg_currencies.csv")
    csv_path = Path(csv_path_str)
    if not csv_path.parent.exists():
        csv_path.parent.mkdir(parents=True, exist_ok=True)
    records = fetch_nbg_currencies(url)
    append_records_to_csv(csv_path, records)
    if not records:
        return Response("", mimetype="text/csv")
    output = io.StringIO()
    fieldnames = list(records[0].keys())
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(records)
    return Response(output.getvalue(), mimetype="text/csv", headers={"Content-Disposition": "attachment; filename=nbg_currencies.csv"})


def main():
    port = int(os.environ.get("PORT", "8000"))
    app.run(host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()

