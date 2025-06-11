from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import googleapiclient.errors
import os
import logging
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
import pickle
from google.auth.transport.requests import Request
import time
import httplib2

# Initialize logging
logging.basicConfig(level=logging.INFO)

# --- CONFIGURATION ---
CLIENT_SECRETS_FILE = 'client_secrets.json'
SERVICE_ACCOUNT_FILE = 'flightstudio-d8c6c3039d4c.json'

SCOPES = [
    'https://www.googleapis.com/auth/youtube.readonly',
    'https://www.googleapis.com/auth/yt-analytics.readonly'
]

PROJECT_ID = 'flightstudio'
DATASET_ID = 'youtube_performance_data'
TABLE_ID = 'episode_duration_retention'

CHANNELS = {
    "UCGq-a57w-aPwyi3pW7XLiHw": "The Diary Of A CEO with Steven Bartlett",
    "UCzt4U1I0Wn-mWwZSOQp9lbQ": "We Need To Talk with Paul C. Brunson",
    "UCvYSNIBgdaYYgdwvWYBmckA": "Begin Again with Davina McCall"
}
# --- END CONFIGURATION ---

def get_credentials(channel_id, channel_name):
    """Get credentials for a specific channel, using cached tokens if available."""
    token_file = f'token_{channel_id}.pickle'
    credentials = None
    
    if os.path.exists(token_file):
        logging.info(f"Loading saved credentials from {token_file}")
        with open(token_file, 'rb') as token:
            credentials = pickle.load(token)
    
    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            logging.info("Refreshing expired credentials")
            credentials.refresh(Request())
        else:
            print(f"\n{'='*50}\nIMPORTANT: OAuth Flow for {channel_name}\n{'='*50}")
            print(f"1. Sign in with the Google account for '{channel_name}'")
            print(f"2. Grant the requested permissions.")
            print(f"{'='*50}\n")
            input(f"Press Enter to start OAuth flow for {channel_name}...")
            
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
            credentials = flow.run_local_server(port=0)
            
            logging.info(f"Saving credentials to {token_file}")
            with open(token_file, 'wb') as token:
                pickle.dump(credentials, token)
    
    return credentials

def get_video_ids(youtube, channel_id, published_after=None, published_before=None):
    """Fetches all video IDs for a given channel within a date range."""
    videos = []
    next_page_token = None
    while True:
        request = youtube.search().list(
            part="id",
            channelId=channel_id,
            maxResults=50,
            type="video",
            order="date",
            publishedAfter=published_after,
            publishedBefore=published_before,
            pageToken=next_page_token
        )
        try:
            response = request.execute()
            for item in response.get('items', []):
                videos.append(item['id']['videoId'])
            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break
        except googleapiclient.errors.HttpError as e:
            logging.error(f"An error occurred fetching video IDs for {channel_id}: {e}")
            break
    logging.info(f"Found {len(videos)} videos for channel {channel_id}.")
    return videos

def get_video_retention_data(youtube_analytics, video_id, channel_id):
    """Fetches audience retention data for a single video with retry logic."""
    end_date = datetime.today().strftime('%Y-%m-%d')
    request = youtube_analytics.reports().query(
        ids=f'channel=={channel_id}',
        startDate='2020-01-01',
        endDate=end_date,
        metrics='relativeRetentionPerformance',
        dimensions='elapsedVideoTimeRatio',
        filters=f'video=={video_id}',
        sort='elapsedVideoTimeRatio'
    )
    
    max_retries = 3
    for attempt in range(max_retries):
    try:
        response = request.execute()
            return response.get('rows', [])
        except (TimeoutError, httplib2.error.ServerNotFoundError) as e:
            logging.warning(f"Network error on video {video_id} ({type(e).__name__}, attempt {attempt + 1}/{max_retries}). Retrying...")
            if attempt < max_retries - 1:
                time.sleep(5 * (attempt + 1))  # Wait 5, then 10 seconds
            else:
                logging.error(f"Failed to retrieve retention for video {video_id} after {max_retries} attempts due to network issues.")
                return None
    except googleapiclient.errors.HttpError as e:
            logging.error(f"Could not retrieve retention for video {video_id}: {e}")
            return None  # Do not retry on other HTTP errors, as they are likely permanent

    return None

def upload_to_bigquery(rows_to_insert):
    """Uploads a list of rows to the configured BigQuery table."""
    if not rows_to_insert:
        logging.info("No new rows to upload to BigQuery.")
        return

    try:
        credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
        client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        
        errors = client.insert_rows_json(table_id, rows_to_insert)
        if not errors:
            logging.info(f"Successfully uploaded {len(rows_to_insert)} rows to BigQuery.")
        else:
            logging.error(f"Encountered errors while inserting rows: {errors}")
    except Exception as e:
        logging.error(f"Failed to upload data to BigQuery: {e}")

def get_processed_video_ids():
    """Gets the set of all video_ids that are already in the BigQuery table."""
    try:
        credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
        client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
        query = f"SELECT DISTINCT video_id FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`"
        query_job = client.query(query)
        return {row.video_id for row in query_job.result()}
    except Exception as e:
        logging.warning(f"Could not fetch existing video IDs from BigQuery: {e}. Assuming no videos are processed.")
        return set()

def main():
    try:
        processed_ids = get_processed_video_ids()
        logging.info(f"Found {len(processed_ids)} videos already processed in BigQuery.")
        
        published_after = "2020-01-01T00:00:00Z"
        published_before = datetime.today().strftime('%Y-%m-%dT%H:%M:%SZ')

        for channel_id, channel_name in CHANNELS.items():
            logging.info(f"\n--- Processing Channel: {channel_name} ---")
            
            credentials = get_credentials(channel_id, channel_name)
            youtube = build('youtube', 'v3', credentials=credentials)
            youtube_analytics = build('youtubeAnalytics', 'v2', credentials=credentials)

        video_ids = get_video_ids(youtube, channel_id, published_after, published_before)
        
            new_videos_to_process = [vid for vid in video_ids if vid not in processed_ids]
            logging.info(f"Found {len(new_videos_to_process)} new videos to process for this channel.")
            
            all_rows_for_bq = []
            for i, video_id in enumerate(new_videos_to_process):
                logging.info(f"Processing video {i+1}/{len(new_videos_to_process)}: {video_id}")
                retention_rows = get_video_retention_data(youtube_analytics, video_id, channel_id)
                if retention_rows:
                    for row in retention_rows:
                        # The API returns relativeRetentionPerformance as a float, so no need for audienceWatchRatio
                        all_rows_for_bq.append({
                            'video_id': video_id,
                            'elapsedVideoTimeRatio': row[0],
                            'relativeRetentionPerformance': row[1]
                        })
            
            if all_rows_for_bq:
                upload_to_bigquery(all_rows_for_bq)

        logging.info("\n--- Audience retention processing complete. ---")
    except Exception as e:
        logging.error(f"An error occurred in the main function: {e}", exc_info=True)

if __name__ == "__main__":
    main()