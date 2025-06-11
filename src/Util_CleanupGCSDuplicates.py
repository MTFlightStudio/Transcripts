import os
import logging
from google.cloud import storage
import re
from collections import defaultdict

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Set the path to your service account key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "flightstudio-d8c6c3039d4c.json"

def list_gcs_files(bucket_name):
    """List all files in a GCS bucket"""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = list(bucket.list_blobs())
    return [blob.name for blob in blobs]

def extract_video_id_and_date(filename):
    """Extract the YouTube video ID and date from filename"""
    # Look for patterns like _xYz123AbCd_20240301
    match = re.search(r'_([A-Za-z0-9_-]{11})_(\d{8})\.mp3$', filename)
    if match:
        return match.group(1), match.group(2)
    return None, None

def find_duplicates(bucket_name):
    """Find duplicate files in GCS bucket based on video ID and date"""
    files = list_gcs_files(bucket_name)
    logging.info(f"Found {len(files)} files in bucket {bucket_name}")
    
    # Group files by video ID + date
    id_date_groups = defaultdict(list)
    id_date_mapping = {}  # Store the original ID and date for reporting
    
    for file in files:
        if not file.endswith('.mp3'):
            continue
        
        video_id, date = extract_video_id_and_date(file)
        if video_id and date:
            key = f"{video_id}|{date}"  # Use pipe as separator instead of underscore
            id_date_groups[key].append(file)
            id_date_mapping[key] = (video_id, date)  # Store for later use
    
    # Filter to only groups with multiple files
    duplicates = {key: files for key, files in id_date_groups.items() if len(files) > 1}
    logging.info(f"Found {len(duplicates)} groups of duplicate files by ID+date")
    
    # Count total duplicates
    total_duplicates = sum(len(files) - 1 for files in duplicates.values())
    logging.info(f"Found {total_duplicates} total duplicate files")
    
    return duplicates, id_date_mapping

def delete_gcs_file(bucket_name, blob_name):
    """Delete a file from GCS bucket"""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.delete()
    logging.info(f"Deleted {blob_name} from bucket {bucket_name}")
    
def cleanup_duplicates(bucket_name, dry_run=True):
    """Clean up duplicate files in GCS bucket"""
    duplicates, id_date_mapping = find_duplicates(bucket_name)
    
    if not duplicates:
        logging.info("No duplicates found.")
        return
    
    # Write report of duplicates
    with open("duplicate_files.txt", "w") as f:
        for key, files in duplicates.items():
            video_id, date = id_date_mapping[key]
            f.write(f"Video ID: {video_id}, Date: {date}\n")
            for i, file in enumerate(files):
                f.write(f"  {i+1}. {file}\n")
            f.write("\n")
    
    logging.info(f"Wrote duplicate report to duplicate_files.txt")
    
    if dry_run:
        logging.info("Dry run: No files were deleted.")
        logging.info("Review duplicate_files.txt and run with dry_run=False to delete duplicates.")
        return
    
    # Delete duplicates - prefer to keep files with underscores
    deleted_count = 0
    for key, files in duplicates.items():
        # Sort by preference: prefer files with underscores over spaces
        def sort_key(filename):
            # Files with underscores are preferred
            space_count = filename.count(' ')
            # Files with fewer special characters are preferred
            special_chars = sum(1 for c in filename if not c.isalnum() and c not in '_-.')
            
            return (space_count, special_chars)
        
        sorted_files = sorted(files, key=sort_key)
        
        # Keep the first (best) file, delete the rest
        for file in sorted_files[1:]:
            delete_gcs_file(bucket_name, file)
            deleted_count += 1
    
    logging.info(f"Deleted {deleted_count} duplicate files")

if __name__ == "__main__":
    bucket_name = "doac_youtube_transcripts"
    
    # First run in dry mode to generate a report
    #cleanup_duplicates(bucket_name, dry_run=True)
    
    # After reviewing the report, uncomment to delete duplicates
    cleanup_duplicates(bucket_name, dry_run=False) 