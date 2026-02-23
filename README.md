## NBG Currency Pipeline

### Overview

This is a simple local pipeline that periodically fetches currency exchange data from the National Bank of Georgia public API (`https://nbg.gov.ge/gw/api/ct/monetarypolicy/currencies/en/json`) and appends it to a CSV file.

### Requirements

- Python 3.10+ installed on your system
- `git` (optional, if you cloned this repository)

### Setup

1. Open a terminal and go to the project directory:

```bash
cd /Users/konstantine25b/Desktop/pipeline_example
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

### Usage

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

