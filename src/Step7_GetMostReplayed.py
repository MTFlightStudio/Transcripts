from dotenv import load_dotenv
import requests
import json
import os
import logging
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import bigquery
import time
from pathlib import Path

# --- CONFIGURATION ---
# Load environment variables from .env file
# Build a path to config.env relative to this script's location for robustness
script_dir = Path(__file__).parent.resolve()
config_path = script_dir / 'config.env'
load_dotenv(dotenv_path=config_path)

# Get SAPISIDHASH from environment variables
SAPISIDHASH = os.getenv("SAPISIDHASH")

# The proxy server URL that the Docker container runs
API_BASE_URL = "http://localhost:8081/videos"

# Path to your service account JSON key file
SERVICE_ACCOUNT_FILE = 'flightstudio-d8c6c3039d4c.json'

# BigQuery configuration
PROJECT_ID = 'flightstudio'
DATASET_ID = 'youtube_performance_data'
TABLE_ID = 'episode_most_replayed'

CHANNELS = {
    "UCGq-a57w-aPwyi3pW7XLiHw": "The Diary Of A CEO with Steven Bartlett",
    "UCzt4U1I0Wn-mWwZSOQp9lbQ": "We Need To Talk with Paul C. Brunson",
    "UCvYSNIBgdaYYgdwvWYBmckA": "Begin Again with Davina McCall"
}
# --- END CONFIGURATION ---

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_youtube_service():
    """Initializes the YouTube API client using service account credentials."""
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=['https://www.googleapis.com/auth/youtube.readonly']
    )
    return build("youtube", "v3", credentials=credentials)

def get_videos_from_channel(youtube, channel_id):
    """Retrieves all video IDs from a channel."""
    videos = []
    next_page_token = None
    while True:
        request = youtube.search().list(
            part="id",
            channelId=channel_id,
            maxResults=50,
            order="date",
            type="video",
            pageToken=next_page_token
        )
        response = request.execute()
        for item in response['items']:
            videos.append(item['id']['videoId'])
        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break
    logging.info(f"Found {len(videos)} videos for channel {channel_id}")
    return videos

def get_video_details(video_id):
    """
    Fetches video details via the local proxy server.
    NOTE: This requires the Docker container proxy to be running.
    """
    if not SAPISIDHASH:
        logging.error("SAPISIDHASH not found. Cannot make request.")
        return None
        
    url = f"{API_BASE_URL}?part=mostReplayed,snippet,statistics&id={video_id}&SAPISIDHASH={SAPISIDHASH}"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        # The proxy often returns non-JSON text before the actual data
        json_start = response.text.find('{')
        if json_start == -1:
            logging.error(f"No JSON object found in response for video {video_id}")
            return None

        clean_json_text = response.text[json_start:]
        data = json.loads(clean_json_text)

        if 'items' in data and len(data['items']) > 0:
            return data['items'][0]  # Return the first item
        else:
            logging.warning(f"Response for {video_id} contained no 'items'.")
            return None
            
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to retrieve data for video {video_id} via proxy: {e}")
        return None
    except json.JSONDecodeError:
        logging.error(f"Failed to parse JSON response for video {video_id} from proxy")
        return None

def get_processed_video_ids(client):
    """Gets the set of all video_ids that are already in the BigQuery table."""
    table_ref = f"`{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`"
    query = f"SELECT DISTINCT video_id FROM {table_ref}"
    try:
        query_job = client.query(query)
        return {row.video_id for row in query_job.result()}
    except Exception as e:
        logging.warning(f"Could not fetch existing video IDs from BigQuery: {e}. Assuming no videos processed.")
        return set()

def upload_to_bigquery(client, rows):
    """Uploads a list of rows to the configured BigQuery table."""
    if not rows:
        logging.info("No new rows to upload to BigQuery.")
        return

    table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    try:
        errors = client.insert_rows_json(table_id, rows)
        if not errors:
            logging.info(f"Successfully uploaded {len(rows)} rows to BigQuery table {TABLE_ID}.")
        else:
            logging.error(f"Encountered errors while inserting rows into BigQuery: {errors}")
    except Exception as e:
        logging.error(f"Failed to upload data to BigQuery: {e}")

def main():
    if not SAPISIDHASH:
        logging.error("SAPISIDHASH is not set in the environment variables. Please set it in config.env.")
        return

    logging.info("Starting Step 7: Get Most Replayed Segments.")
    
    try:
        credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
        bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
        youtube = get_youtube_service()

        processed_ids = get_processed_video_ids(bq_client)
        logging.info(f"Found {len(processed_ids)} videos already processed in BigQuery for most-replayed data.")

        for channel_id, channel_name in CHANNELS.items():
            logging.info(f"\n--- Processing Channel: {channel_name} ---")
            
            video_ids = get_videos_from_channel(youtube, channel_id)
            new_videos_to_process = [vid for vid in video_ids if vid not in processed_ids]
            
            if not new_videos_to_process:
                logging.info("No new videos to process for this channel.")
                continue

            logging.info(f"Found {len(new_videos_to_process)} new videos to process for this channel.")
            channel_rows_for_bq = []

            for i, video_id in enumerate(new_videos_to_process):
                logging.info(f"Processing video {i+1}/{len(new_videos_to_process)}: {video_id}")
                time.sleep(1) 
                
                details = get_video_details(video_id)
                video_segments = []
                if details and 'mostReplayed' in details and details['mostReplayed'] is not None:
                    for marker in details['mostReplayed'].get('markers', []):
                        try:
                            start_ms = int(marker['startMillis'])
                            intensity = float(marker['intensityScoreNormalized'])

                            video_segments.append({
                            "video_id": video_id,
                                "startMillis": start_ms,
                                "startSeconds": int(start_ms / 1000),
                                "intensityScoreNormalized": intensity
                            })
                        except (ValueError, IndexError, KeyError) as e:
                            logging.warning(f"Could not parse marker for video {video_id}: {marker} - Error: {e}")
                
                if video_segments:
                    logging.info(f"SUCCESS: Found {len(video_segments)} most-replayed segments for video {video_id}.")
                    channel_rows_for_bq.extend(video_segments)
                else:
                    logging.info(f"INFO: No most-replayed data returned for video {video_id}.")

            if channel_rows_for_bq:
                upload_to_bigquery(bq_client, channel_rows_for_bq)

        logging.info("\n--- 'Most Replayed' data processing complete. ---")

    except FileNotFoundError:
        logging.error(f"Service account file not found at {SERVICE_ACCOUNT_FILE}.")
    except Exception as e:
        logging.error(f"An unexpected error occurred in main: {e}", exc_info=True)

if __name__ == "__main__":
    main()
