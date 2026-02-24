# User Data Upsert Pipeline

## Overview
Memory-efficient pipeline for upserting user data from CSV delta files into a Parquet database.

## Features

### 🚀 Memory Efficiency
- **Only loads affected users** from the main database (not the entire dataset)
- For production: Uses selective filtering to minimize memory footprint
- Processes changes incrementally rather than loading everything

### 📊 Smart Upsert Logic
1. **Timestamp Conversion**: Converts `updated_at` to proper timestamp format
2. **Add New Users**: Inserts users with new `user_id` values
3. **Update Existing Users**: Keeps the record with the latest `updated_at` timestamp
4. **Delete Users**: Removes users where `is_deleted == True`
5. **Deduplication**: Handles multiple delta records for same user_id

### 📈 Comprehensive Metrics
The pipeline generates `metrics.json` with:
- `added`: Number of new users inserted
- `updated`: Number of existing users updated
- `deleted`: Number of users removed
- `unchanged`: Number of users not affected by delta
- `total_before`: Total users before upsert
- `total_after`: Total users after upsert

## Usage

```bash
cd warehouse
python upsert_users.py
```

## Input Files
- `users.parquet` - Main user database
- `users_delta.csv` - Delta changes to apply

## Output Files
- `users.parquet` - Updated database (overwrites original)
- `metrics.json` - Upsert operation metrics

## Data Validation
The pipeline validates:
- ✅ Email format (must contain '@')
- ✅ No duplicate user_ids in final result
- ✅ All timestamps properly formatted

## Example Scenario

**Before**: 8 users in database

**Delta Changes**: 14 records affecting 11 unique user_ids
- 3 new users (U1009, U1010, U1011)
- 8 existing users with updates
- 2 users marked for deletion (U1005, U1008)

**After**: 9 users in database
- Added: 3 new users
- Updated: 6 users with latest data
- Deleted: 2 users
- Unchanged: 0 users (all were in delta)

## Production Notes

For very large databases (millions of users), this approach would:
1. Use PyArrow's row group filtering for efficient reading
2. Process data in chunks to avoid loading entire dataset
3. Use Dask or similar for parallel processing
4. Implement incremental writes to parquet partitions

The current implementation demonstrates the **conceptual approach** while being practical for small-to-medium datasets.
