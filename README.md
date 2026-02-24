## NBG Currency Pipeline

### Overview

This is a simple local pipeline that periodically fetches currency exchange data from the National Bank of Georgia public API (`https://nbg.gov.ge/gw/api/ct/monetarypolicy/currencies/en/json`) and appends it to a CSV file.

### Requirements

- Python 3.10+ installed on your system
- `git` (optional, if you cloned this repository)

### Setup

1. Open a terminal and go to the project directory:

```bash
cd ./pipeline_example
```

2. Create and activate a virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
```

On Windows (PowerShell):

```bash
python -m venv venv
venv\Scripts\Activate.ps1
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

### Usage (local script with API)

Run the pipeline once (fetch data one time and exit):

```bash
source venv/bin/activate
python nbg_pipeline.py --once
```

Run the pipeline in a loop every 5 minutes (default interval):

```bash
source venv/bin/activate
python nbg_pipeline.py
```

Change the interval (for example, every 1 minute):

```bash
source venv/bin/activate
python nbg_pipeline.py --interval-minutes 1
```

The CSV file will be written to `data/nbg_currencies.csv`. Each run appends the latest values for all currencies, including timestamps.

### Usage (CSV-only script, no API)

If you only want to fetch NBG data and write it to a local CSV file (without starting the Flask API), use `nbg_csv_pipeline.py`:

Run once and exit:

```bash
source venv/bin/activate
python nbg_csv_pipeline.py --once
```

Run in a loop every 5 minutes (default interval):

```bash
source venv/bin/activate
python nbg_csv_pipeline.py
```

Change the interval (for example, every 1 minute):

```bash
source venv/bin/activate
python nbg_csv_pipeline.py --interval-minutes 1
```

By default, the CSV file will be written to `data/nbg_currencies.csv`. You can override this path with `--csv-path`.

### Usage (Historical data - last 2 weeks, replace mode)

If you want to fetch the **last 14 days** of NBG currency data and **replace** the CSV file each time (not append), use `nbg_historical_pipeline.py`:

Run once and exit:

```bash
source venv/bin/activate
python nbg_historical_pipeline.py --once
```

Run in a loop every 24 hours (default interval):

```bash
source venv/bin/activate
python nbg_historical_pipeline.py
```

Change the interval or number of days:

```bash
source venv/bin/activate
python nbg_historical_pipeline.py --interval-hours 12 --days 7
```

By default, the CSV file will be written to `data/nbg_currencies_historical.csv`. You can override this path with `--csv-path`. **Note:** This script **overwrites** the entire file each time it runs (it does not append).

The generated CSV includes a `weekday` column (e.g., "Monday", "Tuesday") derived from the `as_of_date`. NBG publishes rates Monday through Saturday; Sundays are not included (but can be forward-filled using the next script).

### Usage (Forward-fill weekends)

To create a complete dataset with **all 7 days of the week** (where Sunday uses Friday's rates), use `nbg_forward_fill.py`:

```bash
source venv/bin/activate
python nbg_forward_fill.py
```

This reads `data/nbg_currencies_historical.csv` and writes `data/nbg_currencies_filled.csv` with Sunday data forward-filled from the previous Friday.

Custom paths:

```bash
source venv/bin/activate
python nbg_forward_fill.py --input-csv data/nbg_currencies_historical.csv --output-csv data/my_filled_data.csv
```

**Note:** NBG publishes rates Monday-Saturday. This script adds Sunday records by copying Friday's rates with updated `as_of_date` and `weekday` fields.

### Usage as HTTP API

Start the Flask API locally:

```bash
source venv/bin/activate
python nbg_pipeline.py
```

By default it will listen on port `8000` on `localhost`. You can then call:

```bash
curl "http://localhost:8000/currencies"
```

Optional query parameters:

- `url`: override the NBG endpoint
- `csv_path`: change where the CSV is written (default `data/nbg_currencies.csv`)

Each request fetches fresh data from the NBG API, appends it to the CSV file, and returns the JSON records.

Hosted example on Railway (JSON and CSV):

- JSON:

```bash
curl "https://pipelineexample-production.up.railway.app/currencies"
```

- CSV (also writes on the server to `data/nbg_currencies.csv` and downloads locally to `nbg_currencies.csv`):

```bash
curl "https://pipelineexample-production.up.railway.app/currencies.csv?csv_path=data/nbg_currencies.csv" -o nbg_currencies.csv
```

### Run with Docker

Build the Docker image:

```bash
cd /Users/konstantine25b/Desktop/pipeline_example
docker build -t nbg-currency-pipeline .
```

Run the container (default interval 5 minutes):

```bash
docker run --name nbg-currency-pipeline nbg-currency-pipeline
```

Run with a custom interval (for example, every 1 minute):

```bash
docker run --name nbg-currency-pipeline-1m nbg-currency-pipeline python nbg_pipeline.py --interval-minutes 1
```

Persist CSV data on the host machine:

```bash
mkdir -p data
docker run --name nbg-currency-pipeline \
  -v "$(pwd)/data:/app/data" \
  nbg-currency-pipeline
```

You can host this container image on a free container hosting platform (for example, Render, Railway, or Fly.io). For platforms like Railway, set the start command to:

```bash
python nbg_pipeline.py
```

The service will listen on the port provided by the platform (via the `PORT` environment variable) and expose the `/currencies` endpoint for your other applications to call.

