import argparse
import csv
from datetime import datetime, timedelta
from pathlib import Path


DEFAULT_INPUT_CSV = Path("data/nbg_currencies_historical.csv")
DEFAULT_OUTPUT_CSV = Path("data/nbg_currencies_filled.csv")


def parse_iso_date(iso_str):
    if not iso_str:
        return None
    return datetime.fromisoformat(iso_str.replace('Z', '+00:00'))


def forward_fill_weekends(input_csv: Path, output_csv: Path):
    print(f"Reading data from {input_csv}...")
    
    with input_csv.open('r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        records = list(reader)
    
    if not records:
        print("No records found in input file.")
        return
    
    print(f"Read {len(records)} records")
    
    records_by_date = {}
    for record in records:
        as_of_date = record['as_of_date']
        if as_of_date not in records_by_date:
            records_by_date[as_of_date] = []
        records_by_date[as_of_date].append(record)
    
    dates = [parse_iso_date(d) for d in records_by_date.keys()]
    min_date = min(dates).replace(hour=0, minute=0, second=0, microsecond=0)
    max_date = max(dates).replace(hour=0, minute=0, second=0, microsecond=0)
    
    print(f"Date range: {min_date.date()} to {max_date.date()}")
    
    all_records = []
    current_date = min_date
    last_saturday_records = None
    
    while current_date <= max_date:
        iso_date = current_date.isoformat().replace('+00:00', '.000Z')
        weekday = current_date.weekday()
        weekday_name = current_date.strftime("%A")
        
        if iso_date in records_by_date:
            date_records = records_by_date[iso_date]
            for record in date_records:
                record_with_calendar = record.copy()
                record_with_calendar['calendar_date'] = iso_date
                record_with_calendar['calendar_weekday'] = weekday_name
                all_records.append(record_with_calendar)
            
            if weekday == 5:
                last_saturday_records = date_records
            
            print(f"  ✓ {current_date.date()} ({weekday_name}): {len(date_records)} records (original)")
        
        elif weekday in [6, 0]:
            if last_saturday_records:
                for record in last_saturday_records:
                    filled_record = record.copy()
                    filled_record['calendar_date'] = iso_date
                    filled_record['calendar_weekday'] = weekday_name
                    all_records.append(filled_record)
                
                print(f"  → {current_date.date()} ({weekday_name}): {len(last_saturday_records)} records (forward-filled from Saturday)")
            else:
                print(f"  ✗ {current_date.date()} ({weekday_name}): No Saturday data to forward-fill")
        
        current_date += timedelta(days=1)
    
    if not all_records:
        print("No records to write.")
        return
    
    if not output_csv.parent.exists():
        output_csv.parent.mkdir(parents=True, exist_ok=True)
    
    original_fields = list(all_records[0].keys())
    original_fields = [f for f in original_fields if f not in ['calendar_date', 'calendar_weekday']]
    fieldnames = ['calendar_date', 'calendar_weekday'] + original_fields
    
    with output_csv.open('w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_records)
    
    print(f"\n✓ Wrote {len(all_records)} records to {output_csv}")
    
    original_count = len(records)
    filled_count = len(all_records) - original_count
    print(f"  - Original records: {original_count}")
    print(f"  - Forward-filled records: {filled_count}")
    print(f"  - Total records: {len(all_records)}")


def main():
    parser = argparse.ArgumentParser(
        description="Forward-fill NBG currency data: Sunday and Monday use Saturday rates"
    )
    parser.add_argument(
        "--input-csv",
        type=str,
        default=str(DEFAULT_INPUT_CSV),
        help=f"Input CSV file with historical data (default: {DEFAULT_INPUT_CSV})",
    )
    parser.add_argument(
        "--output-csv",
        type=str,
        default=str(DEFAULT_OUTPUT_CSV),
        help=f"Output CSV file with forward-filled data (default: {DEFAULT_OUTPUT_CSV})",
    )
    
    args = parser.parse_args()
    
    input_csv = Path(args.input_csv)
    output_csv = Path(args.output_csv)
    
    if not input_csv.exists():
        print(f"Error: Input file {input_csv} does not exist.")
        print("Run nbg_historical_pipeline.py first to generate the historical data.")
        return
    
    forward_fill_weekends(input_csv, output_csv)
    print("\nDone!")


if __name__ == "__main__":
    main()
