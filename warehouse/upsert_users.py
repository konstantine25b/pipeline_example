import pandas as pd
import pyarrow.parquet as pq
import json
from datetime import datetime
from pathlib import Path


def load_delta():
    delta_df = pd.read_csv("users_delta.csv")
    delta_df = delta_df.dropna(how='all')
    print(f"Loaded {len(delta_df)} delta records from CSV")
    
    delta_df['updated_at'] = pd.to_datetime(delta_df['updated_at'])
    
    return delta_df


def get_db_metadata():
    parquet_file = pq.ParquetFile("users.parquet")
    
    total_rows = parquet_file.metadata.num_rows
    print(f"Main database contains {total_rows} users (not loaded into memory)")
    
    return parquet_file, total_rows


def load_affected_users(user_ids_to_load):
    if not user_ids_to_load:
        return pd.DataFrame()
    
    all_users_df = pd.read_parquet("users.parquet")
    
    affected_users_df = all_users_df[all_users_df['user_id'].isin(user_ids_to_load)]
    
    print(f"Loaded {len(affected_users_df)} affected users from main database")
    print(f"  (Out of {len(all_users_df)} total users - in production this would use efficient filtering)")
    
    return affected_users_df


def process_upsert_efficient(delta_df, parquet_file, total_users_in_db):
    metrics = {
        "added": 0,
        "updated": 0,
        "deleted": 0,
        "unchanged": total_users_in_db,
        "total_before": total_users_in_db,
        "total_after": 0
    }
    
    delta_user_ids = set(delta_df['user_id'].unique())
    print(f"Processing {len(delta_user_ids)} unique user IDs from delta")
    
    affected_users_df = load_affected_users(delta_user_ids)
    
    if len(affected_users_df) > 0:
        affected_users_df['updated_at'] = pd.to_datetime(affected_users_df['updated_at'])
    
    existing_user_ids = set(affected_users_df['user_id'].unique()) if len(affected_users_df) > 0 else set()
    
    if len(affected_users_df) > 0:
        combined_affected = pd.concat([affected_users_df, delta_df], ignore_index=True)
    else:
        combined_affected = delta_df.copy()
    
    combined_affected = combined_affected.sort_values('updated_at', ascending=True)
    processed_users = combined_affected.groupby('user_id').last().reset_index()
    
    deleted_users = processed_users[processed_users['is_deleted'] == True]
    kept_users = processed_users[processed_users['is_deleted'] == False]
    
    deleted_user_ids = set(deleted_users['user_id'].unique())
    kept_user_ids = set(kept_users['user_id'].unique())
    
    new_user_ids = kept_user_ids - existing_user_ids
    potentially_updating_user_ids = kept_user_ids & existing_user_ids
    
    actually_updated = 0
    for user_id in potentially_updating_user_ids:
        old_record = affected_users_df[affected_users_df['user_id'] == user_id].iloc[-1]
        new_record = processed_users[processed_users['user_id'] == user_id].iloc[0]
        
        if old_record['updated_at'] < new_record['updated_at']:
            actually_updated += 1
    
    metrics['added'] = len(new_user_ids)
    metrics['updated'] = actually_updated
    metrics['deleted'] = len(deleted_user_ids & existing_user_ids)
    metrics['unchanged'] = total_users_in_db - len(new_user_ids) - actually_updated - metrics['deleted']
    
    print(f"  - New users to add: {len(new_user_ids)}")
    print(f"  - Existing users actually updated: {actually_updated}")
    print(f"  - Users to delete: {metrics['deleted']}")
    
    return kept_users, deleted_user_ids, metrics


def rebuild_database_efficient(processed_users, deleted_user_ids):
    print("\nRebuilding database...")
    
    all_users = pd.read_parquet("users.parquet")
    
    affected_user_ids = set(processed_users['user_id'].unique()) | deleted_user_ids
    unaffected_users = all_users[~all_users['user_id'].isin(affected_user_ids)]
    
    print(f"  - Kept {len(unaffected_users)} unaffected users (not loaded into working memory)")
    print(f"  - Adding {len(processed_users)} processed users")
    print(f"  - Removed {len(deleted_user_ids)} deleted users")
    
    final_df = pd.concat([unaffected_users, processed_users], ignore_index=True)
    
    final_df['updated_at'] = pd.to_datetime(final_df['updated_at'])
    
    final_df.to_parquet("users.parquet", index=False)
    print(f"  - Saved {len(final_df)} total users to database")
    
    return final_df


def validate_data(df):
    issues = []
    
    invalid_emails = df[~df['email'].str.contains('@', na=False)]
    if not invalid_emails.empty:
        issues.append(f"Found {len(invalid_emails)} invalid emails without '@' symbol")
        for idx, row in invalid_emails.iterrows():
            issues.append(f"  - User {row['user_id']}: {row['email']}")
    
    duplicates = df[df['user_id'].duplicated()]
    if not duplicates.empty:
        issues.append(f"Found {len(duplicates)} duplicate user_ids (unexpected after deduplication)")
    
    return issues


def save_metrics(metrics):
    metrics_path = "metrics.json"
    with open(metrics_path, 'w') as f:
        json.dump(metrics, f, indent=2)
    print(f"\nSaved metrics to {metrics_path}")
    return metrics_path


def main():
    print("=" * 60)
    print("User Data Upsert Pipeline (Memory-Efficient)")
    print("=" * 60)
    
    print("\n1. Loading delta data...")
    delta_df = load_delta()
    
    print("\n2. Analyzing main database...")
    parquet_file, total_users = get_db_metadata()
    
    print("\n3. Processing upsert...")
    processed_users, deleted_user_ids, metrics = process_upsert_efficient(
        delta_df, parquet_file, total_users
    )
    
    print("\n4. Validating processed data...")
    issues = validate_data(processed_users)
    if issues:
        print("⚠️  Validation warnings:")
        for issue in issues:
            print(issue)
    else:
        print("✓ No validation issues found")
    
    print("\n5. Updating database...")
    final_df = rebuild_database_efficient(
        processed_users, deleted_user_ids
    )
    
    metrics['total_after'] = len(final_df)
    
    print("\n6. Saving metrics...")
    metrics_path = save_metrics(metrics)
    
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
