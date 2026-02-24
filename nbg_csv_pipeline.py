import argparse
import time
from pathlib import Path

from nbg_pipeline import fetch_nbg_currencies, append_records_to_csv


DEFAULT_URL = "https://nbg.gov.ge/gw/api/ct/monetarypolicy/currencies/en/json"
DEFAULT_CSV_PATH = Path("data/nbg_currencies.csv")


def run_once(url: str, csv_path: Path) -> None:
    csv_path = Path(csv_path)
    if not csv_path.parent.exists():
        csv_path.parent.mkdir(parents=True, exist_ok=True)
    records = fetch_nbg_currencies(url)
    append_records_to_csv(csv_path, records)


def run_loop(url: str, csv_path: Path, interval_minutes: int) -> None:
    while True:
        run_once(url, csv_path)
        time.sleep(interval_minutes * 60)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch NBG currency data and append to a local CSV file."
    )
    parser.add_argument(
        "--url",
        type=str,
        default=DEFAULT_URL,
        help=f"NBG API URL (default: {DEFAULT_URL})",
    )
    parser.add_argument(
        "--csv-path",
        type=str,
        default=str(DEFAULT_CSV_PATH),
        help=f"Path to CSV file (default: {DEFAULT_CSV_PATH})",
    )
    parser.add_argument(
        "--interval-minutes",
        type=int,
        default=5,
        help="Interval in minutes between fetches (default: 5).",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Fetch data once and exit (no loop).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    csv_path = Path(args.csv_path)

    if args.once:
        run_once(args.url, csv_path)
    else:
        run_loop(args.url, csv_path, args.interval_minutes)


if __name__ == "__main__":
    main()

