import os
import logging
from google.cloud import storage
import re

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

def standardize_filename(filename):
    """Convert underscore style to space style"""
    # Extract the YouTube ID and date
    match = re.search(r'_([A-Za-z0-9_-]{11})_(\d{8})\.mp3$', filename)
    if not match:
        return None, None  # Not a valid file format
        
    youtube_id = match.group(1)
    date = match.group(2)
    
    # Check if it's already in space format
    if ' ' in filename:
        return filename, None  # Already in correct format
    
    # Replace underscores with spaces
    # But need to be careful about the youtube ID and date part
    base_name = filename.split(f"_{youtube_id}_{date}.mp3")[0]
    
    # Convert specific patterns in the base name
    base_name = base_name.replace('__', ': ')  # Double underscore to colon+space
    base_name = base_name.replace('_', ' ')    # Other underscores to spaces
    
    # Fix double spaces
    while '  ' in base_name:
        base_name = base_name.replace('  ', ' ')
    
    new_filename = f"{base_name}_{youtube_id}_{date}.mp3"
    
    return new_filename, filename

def rename_gcs_file(bucket_name, old_name, new_name):
    """Rename a file in GCS bucket (copy and delete)"""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    # Get the blob to copy
    blob = bucket.blob(old_name)
    
    # Copy to new name
    bucket.copy_blob(blob, bucket, new_name)
    logging.info(f"Copied {old_name} to {new_name}")
    
    # Delete the old blob
    blob.delete()
    logging.info(f"Deleted {old_name}")

def standardize_all_files(bucket_name, dry_run=True):
    """Standardize all filenames in bucket to match preferred format"""
    files = list_gcs_files(bucket_name)
    logging.info(f"Found {len(files)} files in bucket {bucket_name}")
    
    # Find files to rename
    to_rename = []
    for file in files:
        if not file.endswith('.mp3'):
            continue
            
        new_name, old_name = standardize_filename(file)
        if old_name:  # If old_name is not None, it needs renaming
            to_rename.append((old_name, new_name))
    
    logging.info(f"Found {len(to_rename)} files to rename")
    
    # Write report
    with open("renaming_plan.txt", "w") as f:
        for old_name, new_name in to_rename:
            f.write(f"From: {old_name}\n")
            f.write(f"To:   {new_name}\n")
            f.write("\n")
    
    logging.info(f"Wrote renaming plan to renaming_plan.txt")
    
    if dry_run:
        logging.info("Dry run: No files were renamed.")
        logging.info("Review renaming_plan.txt and run with dry_run=False to rename files.")
        return
    
    # Rename files
    for old_name, new_name in to_rename:
        rename_gcs_file(bucket_name, old_name, new_name)
    
    logging.info(f"Renamed {len(to_rename)} files")

if __name__ == "__main__":
    bucket_name = "doac_youtube_transcripts"
    
    # First run in dry mode to generate a plan
    standardize_all_files(bucket_name, dry_run=True)
    
    # After reviewing the plan, uncomment to execute renames
    # standardize_all_files(bucket_name, dry_run=False) 