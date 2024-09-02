import os
import logging
import yt_dlp as youtube_dl
from time import sleep
from google.cloud import storage  # Import Google Cloud Storage library
from datetime import datetime  # Import datetime module

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
        
        ydl_opts = {
            'format': 'bestaudio/best',
            'outtmpl': os.path.join(output_path, '%(id)s.%(ext)s'),
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '192',
            }],
            'ffmpeg_location': '/opt/homebrew/bin'  # Update this path to where ffmpeg is installed
        }
        
        with youtube_dl.YoutubeDL(ydl_opts) as ydl:
            ydl.download([video_url])
            info_dict = ydl.extract_info(video_url, download=False)
            audio_file = os.path.join(output_path, f"{info_dict['id']}.mp3")
            video_title = info_dict.get('title', 'unknown_title')  # Extract the video title
            upload_date = info_dict.get('upload_date', 'unknown_date')  # Extract the upload date
        
        logging.info(f"Downloaded and saved audio for video ID: {video_id} to {audio_file}")
        return audio_file, video_title, upload_date  # Return the audio file path, video title, and upload date
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
    except Exception as e:
        logging.error(f"Error uploading {source_file_name} to GCS: {e}", exc_info=True)

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
    audio_file, video_title, upload_date = download_audio_from_youtube(video_id, output_path)
    if audio_file:
        # Use the video title, video ID, and upload date in the destination blob name
        destination_blob_name = f"{video_title}_{video_id}_{upload_date}.mp3"
        
        # Check if the file already exists in GCS
        if file_exists_in_gcs(bucket_name, destination_blob_name):
            logging.info(f"File {destination_blob_name} already exists in bucket {bucket_name}. Skipping download and upload.")
            return
        
        upload_to_gcs(bucket_name, audio_file, destination_blob_name)
        os.remove(audio_file)  # Remove the local file to save space
        logging.info(f"Audio file {audio_file} removed from local storage")

# Load the list of video IDs from the temporary file
def load_video_ids(temp_file):
    logging.info(f"Loading video IDs from {temp_file}")
    with open(temp_file, 'r') as f:
        video_ids = [line.strip() for line in f]
    logging.info(f"Loaded {len(video_ids)} video IDs")
    return video_ids


if __name__ == "__main__":
    # Path to the temp file where video_ids are listed
    temp_file = "video_ids.txt"  # Adjust the path as necessary
    bucket_name = "doac_youtube_transcripts"  # Replace with your GCS bucket name
    
    # Load the list of video IDs
    video_ids = load_video_ids(temp_file)
    
    # Download and upload each video to GCS
    for video_id in video_ids:
        download_and_upload_to_gcs(video_id, bucket_name)
    
    logging.info("All downloads and uploads to GCS completed")