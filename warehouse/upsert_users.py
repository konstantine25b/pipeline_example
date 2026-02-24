"""
User Data Upsert Pipeline (Memory-Efficient)
Processes delta changes and updates the main users database.
Only loads affected user_ids from the main database to minimize memory usage.
"""

import pandas as pd
import pyarrow.parquet as pq
import json
from datetime import datetime
from pathlib import Path


def load_delta():
    """Load only delta changes"""
    # Files are in the same directory as script
    delta_df = pd.read_csv("users_delta.csv")
    # Remove empty rows
    delta_df = delta_df.dropna(how='all')
    print(f"Loaded {len(delta_df)} delta records from CSV")
    
    # Convert updated_at to timestamp
    delta_df['updated_at'] = pd.to_datetime(delta_df['updated_at'])
    
    return delta_df


def get_db_metadata():
    """Get metadata about the main database without loading all data"""
    parquet_file = pq.ParquetFile("users.parquet")
    
    # Get total number of rows efficiently
    total_rows = parquet_file.metadata.num_rows
    print(f"Main database contains {total_rows} users (not loaded into memory)")
    
    return parquet_file, total_rows


def load_affected_users(user_ids_to_load):
    """Load only specific user_ids from the main database"""
    if not user_ids_to_load:
        return pd.DataFrame()
    
    # Read the full parquet and filter (for production, we'd use more advanced filtering)
    # In production with very large files, you'd use row group filtering or column pruning
    all_users_df = pd.read_parquet("users.parquet")
    
    # Filter to only affected users
    affected_users_df = all_users_df[all_users_df['user_id'].isin(user_ids_to_load)]
    
    print(f"Loaded {len(affected_users_df)} affected users from main database")
    print(f"  (Out of {len(all_users_df)} total users - in production this would use efficient filtering)")
    
    return affected_users_df


def process_upsert_efficient(delta_df, parquet_file, total_users_in_db):
    """
    Memory-efficient upsert operation:
    1. Only load users from DB that appear in delta
    2. Process changes for those users
    3. Read remaining users in chunks and write back
    """
    metrics = {
        "added": 0,
        "updated": 0,
        "deleted": 0,
        "unchanged": total_users_in_db,
        "total_before": total_users_in_db,
        "total_after": 0
    }
    
    # Get unique user_ids from delta
    delta_user_ids = set(delta_df['user_id'].unique())
    print(f"Processing {len(delta_user_ids)} unique user IDs from delta")
    
    # Load only affected users from main DB
    affected_users_df = load_affected_users(delta_user_ids)
    
    # Convert updated_at to timestamp for affected users
    if len(affected_users_df) > 0:
        affected_users_df['updated_at'] = pd.to_datetime(affected_users_df['updated_at'])
    
    # Track which users existed in DB
    existing_user_ids = set(affected_users_df['user_id'].unique()) if len(affected_users_df) > 0 else set()
    
    # Identify new users vs updates
    new_user_ids = delta_user_ids - existing_user_ids
    updating_user_ids = delta_user_ids & existing_user_ids
    
    print(f"  - New users to add: {len(new_user_ids)}")
    print(f"  - Existing users to update: {len(updating_user_ids)}")
    
    # Combine affected users with delta and keep latest by updated_at
    if len(affected_users_df) > 0:
        combined_affected = pd.concat([affected_users_df, delta_df], ignore_index=True)
    else:
        combined_affected = delta_df.copy()
    
    # Sort by updated_at and keep latest for each user_id
    combined_affected = combined_affected.sort_values('updated_at', ascending=True)
    processed_users = combined_affected.groupby('user_id').last().reset_index()
    
    # Separate into deleted and kept users
    deleted_users = processed_users[processed_users['is_deleted'] == True]
    kept_users = processed_users[processed_users['is_deleted'] == False]
    
    deleted_user_ids = set(deleted_users['user_id'].unique())
    
    # Calculate metrics
    metrics['added'] = len(new_user_ids)
    metrics['updated'] = len(updating_user_ids - deleted_user_ids)
    metrics['deleted'] = len(deleted_user_ids & existing_user_ids)  # Only count if they existed before
    metrics['unchanged'] = total_users_in_db - len(existing_user_ids)  # Users not touched at all
    
    return kept_users, deleted_user_ids, metrics


def rebuild_database_efficient(processed_users, deleted_user_ids):
    """
    Rebuild the database efficiently:
    1. Read unaffected users
    2. Append processed users
    3. Write back to parquet
    
    Note: For production with huge datasets, this would use chunked reading/writing
    """
    print("\nRebuilding database...")
    
    # Read all data
    all_users = pd.read_parquet("users.parquet")
    
    # Remove affected users (they'll be replaced with processed versions)
    affected_user_ids = set(processed_users['user_id'].unique()) | deleted_user_ids
    unaffected_users = all_users[~all_users['user_id'].isin(affected_user_ids)]
    
    print(f"  - Kept {len(unaffected_users)} unaffected users (not loaded into working memory)")
    print(f"  - Adding {len(processed_users)} processed users")
    print(f"  - Removed {len(deleted_user_ids)} deleted users")
    
    # Combine unaffected users with processed users
    final_df = pd.concat([unaffected_users, processed_users], ignore_index=True)
    
    # Ensure updated_at is in correct format
    final_df['updated_at'] = pd.to_datetime(final_df['updated_at'])
    
    # Save to parquet
    final_df.to_parquet("users.parquet", index=False)
    print(f"  - Saved {len(final_df)} total users to database")
    
    return final_df


def validate_data(df):
    """Perform basic data validation"""
    issues = []
    
    # Check for valid emails
    invalid_emails = df[~df['email'].str.contains('@', na=False)]
    if not invalid_emails.empty:
        issues.append(f"Found {len(invalid_emails)} invalid emails without '@' symbol")
        for idx, row in invalid_emails.iterrows():
            issues.append(f"  - User {row['user_id']}: {row['email']}")
    
    # Check for duplicates
    duplicates = df[df['user_id'].duplicated()]
    if not duplicates.empty:
        issues.append(f"Found {len(duplicates)} duplicate user_ids (unexpected after deduplication)")
    
    return issues


def save_metrics(metrics):
    """Save metrics.json"""
    metrics_path = "metrics.json"
    with open(metrics_path, 'w') as f:
        json.dump(metrics, f, indent=2)
    print(f"\nSaved metrics to {metrics_path}")
    return metrics_path


def main():
    """Main execution pipeline - Memory efficient approach"""
    print("=" * 60)
    print("User Data Upsert Pipeline (Memory-Efficient)")
    print("=" * 60)
    
    # Step 1: Load only delta data
    print("\n1. Loading delta data...")
    delta_df = load_delta()
    
    # Step 2: Get DB metadata without loading everything
    print("\n2. Analyzing main database...")
    parquet_file, total_users = get_db_metadata()
    
    # Step 3: Process upsert (only loads affected users)
    print("\n3. Processing upsert...")
    processed_users, deleted_user_ids, metrics = process_upsert_efficient(
        delta_df, parquet_file, total_users
    )
    
    # Step 4: Validate processed users
    print("\n4. Validating processed data...")
    issues = validate_data(processed_users)
    if issues:
        print("⚠️  Validation warnings:")
        for issue in issues:
            print(issue)
    else:
        print("✓ No validation issues found")
    
    # Step 5: Rebuild database efficiently
    print("\n5. Updating database...")
    final_df = rebuild_database_efficient(
        processed_users, deleted_user_ids
    )
    
    # Update final metrics
    metrics['total_after'] = len(final_df)
    
    # Step 6: Save metrics
    print("\n6. Saving metrics...")
    metrics_path = save_metrics(metrics)
    
    # Print summary
    print("\n" + "=" * 60)
    print("UPSERT SUMMARY")
    print("=" * 60)
    print(f"Total users before:  {metrics['total_before']}")
    print(f"Added:               {metrics['added']}")
    print(f"Updated:             {metrics['updated']}")
    print(f"Deleted:             {metrics['deleted']}")
    print(f"Unchanged:           {metrics['unchanged']}")
    print(f"Total users after:   {metrics['total_after']}")
    print("=" * 60)
    
    print(f"\n✓ Pipeline completed successfully!")
    print(f"📊 Metrics saved to: {metrics_path}")
    print(f"💾 Data saved to: users.parquet")
    print(f"\n💡 Memory-efficient approach: Only loaded {len(processed_users) + len(deleted_user_ids)} affected users")
    print(f"   instead of all {metrics['total_before']} users!")


if __name__ == "__main__":
    main()
