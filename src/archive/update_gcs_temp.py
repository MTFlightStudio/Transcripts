import os
import logging
from google.cloud import storage
from google.oauth2 import service_account
import googleapiclient.discovery

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Set the path to your service account key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "flightstudio-d8c6c3039d4c.json"

# Path to your service account JSON key file
SERVICE_ACCOUNT_FILE = 'flightstudio-d8c6c3039d4c.json'  # Update to your JSON key file path

# Initialize the YouTube API client using service account credentials
def get_youtube_service():
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=['https://www.googleapis.com/auth/youtube.readonly']
    )
    youtube = googleapiclient.discovery.build(
        "youtube", "v3", credentials=credentials)
    
    return youtube

# Function to load video IDs from a file
def load_video_ids(temp_file):
    logging.info(f"Loading video IDs from {temp_file}")
    with open(temp_file, 'r') as f:
        video_ids = [line.strip() for line in f]
    logging.info(f"Loaded {len(video_ids)} video IDs")
    return video_ids

# Function to get video details from YouTube API
def get_video_details(video_id):
    youtube = get_youtube_service()
    request = youtube.videos().list(part='snippet', id=video_id)
    response = request.execute()
    
    if 'items' in response and len(response['items']) > 0:
        video = response['items'][0]
        title = video['snippet']['title']
        upload_date = video['snippet']['publishedAt'][:10].replace('-', '')
        return title, upload_date
    else:
        logging.warning(f"No details found for video ID {video_id}")
        return None, None

# Function to rename files in GCS
def rename_gcs_files(bucket_name, video_ids):
    logging.info(f"Renaming files in GCS bucket {bucket_name}")
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        
        blobs = bucket.list_blobs()
        for blob in blobs:
            logging.info(f"Processing blob: {blob.name}")
            # Extract video title and upload date from the current blob name
            parts = blob.name.rsplit('_', 1)
            if len(parts) == 2 and parts[1].endswith('.mp3'):
                video_title = parts[0]
                upload_date = parts[1].replace('.mp3', '')
                
                # Find the corresponding video ID
                for video_id in video_ids:
                    title, date = get_video_details(video_id)
                    if title and date and title in video_title:
                        new_blob_name = f"{video_title}_{video_id}_{upload_date}.mp3"
                        
                        # Copy and delete to rename the blob
                        new_blob = bucket.copy_blob(blob, bucket, new_blob_name)
                        bucket.delete_blob(blob.name)
                        logging.info(f"Renamed {blob.name} to {new_blob.name}")
                        break
                else:
                    logging.warning(f"No matching video ID found for blob {blob.name}")
            else:
                logging.warning(f"Skipping blob {blob.name} as it does not match the expected format")
    except Exception as e:
        logging.error(f"Error renaming files in GCS: {e}", exc_info=True)

if __name__ == "__main__":
    # Path to the temp file where video_ids are listed
    temp_file = "video_ids.txt"  # Adjust the path as necessary
    bucket_name = "doac_youtube_transcripts"  # Replace with your GCS bucket name
    
    # Load the list of video IDs
    video_ids = load_video_ids(temp_file)
    
    # Rename files in GCS
    rename_gcs_files(bucket_name, video_ids)
    
    logging.info("All file renaming in GCS completed")
