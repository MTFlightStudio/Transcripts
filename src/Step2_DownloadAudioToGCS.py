import os
import logging
import yt_dlp
from time import sleep
import random
from google.cloud import storage
from datetime import datetime
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Set the path to your service account key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "flightstudio-d8c6c3039d4c.json"

# Function to download audio from a YouTube video
def download_audio_from_youtube(video_id, output_path="audio"):
    logging.info(f"Starting download for video ID: {video_id}")
    try:
        video_url = f"https://www.youtube.com/watch?v={video_id}"
        logging.info(f"Constructed YouTube URL: {video_url}")
        
        # Create output directory if it doesn't exist
        if not os.path.exists(output_path):
            os.makedirs(output_path)
            logging.info(f"Created output directory: {output_path}")
        
        cookies_file = "youtube_cookies_netscape.txt"
        
        # First, check if the video has audio formats
        with yt_dlp.YoutubeDL({
            'quiet': True,
            'cookiefile': cookies_file,
            'skip_download': True  # Just get info, don't download
        }) as ydl:
            info_dict = ydl.extract_info(video_url, download=False)
            
            if not info_dict:
                logging.error(f"Could not extract info for video {video_id}")
                return None, None, None
                
            # Check if there are any formats with audio
            has_audio = any(format.get('acodec') != 'none' for format in info_dict.get('formats', []))
            
            if not has_audio:
                logging.warning(f"Video {video_id} has no audio formats available. Skipping.")
                return None, None, None
            
            video_title = info_dict.get('title', 'unknown_title')
            upload_date = info_dict.get('upload_date', 'unknown_date')
            
        # If we have audio formats, download the best one
        ydl_opts = {
            'format': 'bestaudio/best',
            'outtmpl': os.path.join(output_path, '%(id)s.%(ext)s'),
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '192',
            }],
            'ffmpeg_location': '/opt/homebrew/bin',
            'cookiefile': cookies_file,
            'ignoreerrors': True,
            'no_warnings': False
        }
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([video_url])
            # Check if the file was actually downloaded
            expected_file = os.path.join(output_path, f"{video_id}.mp3")
            if os.path.exists(expected_file):
                logging.info(f"Downloaded and saved audio for video ID: {video_id} to {expected_file}")
                return expected_file, video_title, upload_date
            else:
                logging.error(f"Failed to download audio for {video_id} - file not created")
                return None, None, None
                
    except Exception as e:
        logging.error(f"Error downloading {video_id}: {e}", exc_info=True)
        return None, None, None

# Function to upload a file to Google Cloud Storage
def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    logging.info(f"Uploading {source_file_name} to GCS bucket {bucket_name} as {destination_blob_name}")
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)
        logging.info(f"File {source_file_name} uploaded to {destination_blob_name} in bucket {bucket_name}")
        return True
    except Exception as e:
        logging.error(f"Error uploading {source_file_name} to GCS: {e}", exc_info=True)
        return False

# Function to check if a file exists in the GCS bucket
def file_exists_in_gcs(bucket_name, destination_blob_name):
    logging.info(f"Checking if {destination_blob_name} exists in GCS bucket {bucket_name}")
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        return blob.exists()
    except Exception as e:
        logging.error(f"Error checking if {destination_blob_name} exists in GCS: {e}", exc_info=True)
        return False

# Function to download and upload audio to GCS
def download_and_upload_to_gcs(video_id, bucket_name, output_path="audio"):
    # Clean destination filename for GCS (remove invalid characters)
    def clean_filename(filename):
        if not filename:
            return "unknown_title"
        
        # First, replace spaces with underscores
        filename = filename.replace(' ', '_')
        
        # Replace common special characters with simpler versions
        replacements = {
            ':': '',
            '|': '',
            '$': 's',
            '&': 'and',
            "'": '',
            '"': '',
            '!': '',
            '?': '',
            ',': '',
            '.': '',
            '/': '_',
            '\\': '_',
            '(': '',
            ')': '',
            '[': '',
            ']': '',
            '{': '',
            '}': '',
            '@': 'at',
            '#': '',
            '%': 'percent',
            '+': 'plus',
            '=': 'equals',
            '<': '',
            '>': '',
            '*': '',
            ';': '',
        }
        
        for char, replacement in replacements.items():
            filename = filename.replace(char, replacement)
        
        # Replace any remaining non-alphanumeric chars with underscores
        filename = ''.join(c if c.isalnum() or c in '-_' else '_' for c in filename)
        
        # Remove multiple consecutive underscores
        while '__' in filename:
            filename = filename.replace('__', '_')
        
        # Remove leading/trailing underscores
        filename = filename.strip('_')
        
        return filename
    
    audio_file, video_title, upload_date = download_audio_from_youtube(video_id, output_path)
    if audio_file:
        # Clean the video title to ensure valid GCS object name
        clean_title = clean_filename(video_title)
        destination_blob_name = f"{clean_title}_{video_id}_{upload_date}.mp3"
        
        # Check if the file already exists in GCS
        if file_exists_in_gcs(bucket_name, destination_blob_name):
            logging.info(f"File {destination_blob_name} already exists in bucket {bucket_name}. Skipping upload.")
            success = True
        else:
            success = upload_to_gcs(bucket_name, audio_file, destination_blob_name)
        
        # Remove the local file to save space
        try:
            os.remove(audio_file)
            logging.info(f"Audio file {audio_file} removed from local storage")
        except Exception as e:
            logging.warning(f"Could not remove local file {audio_file}: {e}")
        
        return success
    else:
        logging.warning(f"No audio file was downloaded for {video_id}. Skipping upload.")
        return False

# Load the list of video IDs from the temporary file
def load_video_ids(temp_file):
    logging.info(f"Loading video IDs from {temp_file}")
    try:
        with open(temp_file, 'r') as f:
            video_ids = [line.strip() for line in f if line.strip()]
        logging.info(f"Loaded {len(video_ids)} video IDs")
        return video_ids
    except Exception as e:
        logging.error(f"Error loading video IDs from {temp_file}: {e}")
        return []

def list_gcs_files(bucket_name):
    """List all files in a GCS bucket"""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = list(bucket.list_blobs())
    return [blob.name for blob in blobs]

def get_video_ids_from_gcs(bucket_name):
    """Extract video IDs from existing files in GCS"""
    files = list_gcs_files(bucket_name)
    logging.info(f"Found {len(files)} files in bucket {bucket_name}")
    
    # Extract YouTube ID from filename
    video_ids = set()
    for file in files:
        if not file.endswith('.mp3'):
            continue
            
        match = re.search(r'_([A-Za-z0-9_-]{11})_\d{8}\.mp3$', file)
        if match:
            youtube_id = match.group(1)
            video_ids.add(youtube_id)
    
    logging.info(f"Extracted {len(video_ids)} unique video IDs from bucket")
    return video_ids

def filter_new_videos(input_file, bucket_name):
    """Filter list to only videos not already in GCS"""
    # Get existing video IDs
    existing_ids = get_video_ids_from_gcs(bucket_name)
    
    # Read input file
    with open(input_file, 'r') as f:
        input_ids = [line.strip() for line in f if line.strip()]
    
    logging.info(f"Found {len(input_ids)} video IDs in input file")
    
    # Filter to new videos only
    new_videos = [vid for vid in input_ids if vid not in existing_ids]
    
    logging.info(f"{len(new_videos)} videos need to be downloaded")
    logging.info(f"{len(input_ids) - len(new_videos)} videos already exist in bucket")
    
    # Write new videos to file
    if new_videos:
        with open('new_videos.txt', 'w') as f:
            for video_id in new_videos:
                f.write(f"{video_id}\n")
        logging.info(f"Wrote {len(new_videos)} new video IDs to new_videos.txt")
    
    return new_videos

if __name__ == "__main__":
    bucket_name = "doac_youtube_transcripts"
    input_file = "video_ids.txt"  # Default input file
    
    # Check if input file exists
    if not os.path.exists(input_file):
        logging.error(f"Input file {input_file} not found!")
        exit(1)
    
    # Filter to only new videos
    new_videos = filter_new_videos(input_file, bucket_name)
    
    if not new_videos:
        logging.info("No new videos to download. All videos already exist in bucket.")
    else:
        logging.info(f"Found {len(new_videos)} new videos in new_videos.txt. Starting download and upload process.")
        
        # Use the 'new_videos.txt' as the source of IDs to process
        video_ids_to_process = load_video_ids('new_videos.txt') # load_video_ids can be reused

        if not video_ids_to_process:
            logging.info("new_videos.txt is empty or could not be read. No videos to process.")
        else:
            processed_count = 0
            failed_count = 0
            for i, video_id in enumerate(video_ids_to_process):
                logging.info(f"Processing video ID {i+1}/{len(video_ids_to_process)}: {video_id}")
                # Add a random delay between 30 and 120 seconds
                # only add delay if it's not the first video
                if i > 0:
                    delay = random.randint(30, 120)
                    logging.info(f"Waiting for {delay} seconds before next download...")
                    sleep(delay)
                
                success = download_and_upload_to_gcs(video_id, bucket_name)
                if success:
                    processed_count += 1
                else:
                    failed_count += 1
                logging.info(f"Finished processing video ID: {video_id}. Success: {success}")

            logging.info(f"Download and upload process completed.")
            logging.info(f"Successfully processed {processed_count} videos.")
            if failed_count > 0:
                logging.warning(f"Failed to process {failed_count} videos. Check logs for details.")