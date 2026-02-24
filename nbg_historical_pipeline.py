import argparse
import csv
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests


DEFAULT_CSV_PATH = Path("data/nbg_currencies_historical.csv")


def fetch_nbg_currencies_by_date(date_str: str):
    """
    Fetch NBG currency data for a specific date.
    Date format: YYYY-MM-DD
    Example URL: https://nbg.gov.ge/gw/api/ct/monetarypolicy/currencies/en/json/?date=2024-02-20
    """
    url = f"https://nbg.gov.ge/gw/api/ct/monetarypolicy/currencies/en/json/?date={date_str}"
    try:
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
        
        # Parse as_of_date to get weekday name
        if as_of_date:
            # Parse ISO format date string to datetime
            date_obj = datetime.fromisoformat(as_of_date.replace('Z', '+00:00'))
            weekday_name = date_obj.strftime("%A")  # e.g., "Monday", "Tuesday"
        else:
            weekday_name = "Unknown"
        
        for c in currencies:
            record = {
                "fetched_at": fetched_at,
                "as_of_date": as_of_date,
                "weekday": weekday_name,
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
    except Exception as e:
        print(f"Error fetching data for {date_str}: {e}")
        return []


def fetch_last_n_days(days: int = 14):
    """
    Fetch NBG currency data for the last N days.
    Note: NBG API returns the most recent available rate for weekends/holidays.
    Deduplication is handled in write_records_to_csv().
    Returns a list of all records combined.
    """
    all_records = []
    today = datetime.now()
    
    for i in range(days):
        target_date = today - timedelta(days=i)
        date_str = target_date.strftime("%Y-%m-%d")
        print(f"Fetching data for {date_str}...")
        records = fetch_nbg_currencies_by_date(date_str)
        if records:
            all_records.extend(records)
            # Show what date the API actually returned
            actual_date = records[0].get('as_of_date', 'unknown')[:10] if records else 'unknown'
            print(f"  ✓ Fetched {len(records)} currency records (as_of_date: {actual_date})")
        else:
            print(f"  ✗ No data available for {date_str}")
        
        # Small delay to avoid overwhelming the API
        time.sleep(0.5)
    
    return all_records


def write_records_to_csv(csv_path: Path, records):
    """
    Write records to CSV, replacing the entire file.
    Deduplicates records by (as_of_date, code) to avoid duplicate entries
    when NBG API returns the same date for multiple requests (e.g., weekends).
    """
    if not records:
        print("No records to write.")
        return
    
    # Deduplicate by (as_of_date, code) - keep the first occurrence
    # Note: weekday is derived from as_of_date, so no need to include in key
    seen = set()
    unique_records = []
    for record in records:
        key = (record.get('as_of_date'), record.get('code'))
        if key not in seen:
            seen.add(key)
            unique_records.append(record)
    
    if len(records) != len(unique_records):
        duplicates = len(records) - len(unique_records)
        print(f"ℹ Removed {duplicates} duplicate records")
    
    # Ensure directory exists
    if not csv_path.parent.exists():
        csv_path.parent.mkdir(parents=True, exist_ok=True)
    
    fieldnames = list(unique_records[0].keys())
    
    # Overwrite the file (mode='w')
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(unique_records)
    
    print(f"✓ Wrote {len(unique_records)} unique records to {csv_path}")


def run_once(csv_path: Path, days: int) -> None:
    """
    Fetch last N days of data and replace the CSV file.
    """
    csv_path = Path(csv_path)
    print(f"Fetching last {days} days of NBG currency data...")
    records = fetch_last_n_days(days)
    write_records_to_csv(csv_path, records)
    print("Done!")


def run_loop(csv_path: Path, days: int, interval_hours: int) -> None:
    """
    Run in a loop, fetching data and replacing the CSV at specified intervals.
    """
    print(f"Starting historical data pipeline (interval: {interval_hours} hours, last {days} days)")
    while True:
        run_once(csv_path, days)
        print(f"Sleeping for {interval_hours} hour(s)...")
        time.sleep(interval_hours * 3600)


def main():
    parser = argparse.ArgumentParser(
        description="Fetch last N days of NBG currency data and replace CSV file"
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run once and exit (default is to run in a loop)",
    )
    parser.add_argument(
        "--csv-path",
        type=str,
        default=str(DEFAULT_CSV_PATH),
        help=f"Path to CSV file (default: {DEFAULT_CSV_PATH})",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=14,
        help="Number of days to fetch (default: 14)",
    )
    parser.add_argument(
        "--interval-hours",
        type=int,
        default=24,
        help="Interval in hours between fetches when running in loop mode (default: 24)",
    )

    args = parser.parse_args()
    csv_path = Path(args.csv_path)

    if args.once:
        run_once(csv_path, args.days)
    else:
        run_loop(csv_path, args.days, args.interval_hours)


if __name__ == "__main__":
    main()
