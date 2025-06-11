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

def extract_yt_info(filename):
    """Extract YouTube ID and date from filename"""
    # Try to extract YouTube ID and date
    match = re.search(r'_([A-Za-z0-9_-]{11})_(\d{8})\.mp3$', filename)
    if match:
        youtube_id = match.group(1)
        date = match.group(2)
        title_part = filename[:-len(f"_{youtube_id}_{date}.mp3")]
        return title_part, youtube_id, date
    return None, None, None

def improve_title(title):
    """Better title conversion from underscore to space format"""
    # Replace underscores with spaces
    title = title.replace('_', ' ')
    
    # Fix apostrophes
    title = re.sub(r' s ', "'s ", title)
    title = re.sub(r' t ', "'t ", title)
    title = re.sub(r' re ', "'re ", title)
    title = re.sub(r' ve ', "'ve ", title)
    title = re.sub(r' ll ', "'ll ", title)
    title = re.sub(r' m ', "'m ", title)
    
    # Fix any double spaces
    while '  ' in title:
        title = title.replace('  ', ' ')
    
    # Fix colons (often used in titles)
    title = title.replace(' : ', ': ')
    
    # Fix any leading/trailing spaces
    title = title.strip()
    
    return title

def standardize_filename(filename):
    """Convert underscore style to space style, properly"""
    # Skip already processed files - more safely
    if ' ' in filename and len(filename.split('_')) >= 3:
        # Check if it looks like it has spaces in title but underscore format at end
        # This is likely already processed
        parts = filename.split('_')
        if parts[-1].endswith('.mp3') and parts[-2].isdigit() and len(parts[-2]) == 8:
            return None, None  # Already processed
        
    title_part, youtube_id, date = extract_yt_info(filename)
    if not title_part or not youtube_id or not date:
        return None, None  # Invalid format
    
    # Create new filename with improved title and original ID/date
    new_title = improve_title(title_part)
    new_filename = f"{new_title}_{youtube_id}_{date}.mp3"
    
    # Only return if there's a real change
    if new_filename != filename:
        return new_filename, filename
    return None, None

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
    

    
    # After reviewing the plan, uncomment to execute renames
    standardize_all_files(bucket_name, dry_run=False) 