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

# Initialize logging
logging.basicConfig(level=logging.INFO)

# Path to your client secrets JSON file
CLIENT_SECRETS_FILE = 'client_secrets.json'
# Path to your service account key file
SERVICE_ACCOUNT_FILE = 'flightstudio-d8c6c3039d4c.json'

# Scopes required for the YouTube API
SCOPES = [
    'https://www.googleapis.com/auth/youtube.readonly',
    'https://www.googleapis.com/auth/yt-analytics.readonly'
]

# BigQuery configuration
PROJECT_ID = 'flightstudio'
DATASET_ID = 'youtube_performance_data'
TABLE_ID = 'episode_duration_retention'

def get_service():
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
    flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
    credentials = flow.run_local_server(port=0)  # Use local server-based flow
    youtube = build('youtube', 'v3', credentials=credentials)
    youtubeAnalytics = build('youtubeAnalytics', 'v2', credentials=credentials)
    return youtube, youtubeAnalytics, credentials

def get_credentials():
    try:
        logging.info("Starting OAuth flow...")
        youtube, youtubeAnalytics, credentials = get_service()
        logging.info("OAuth flow completed successfully.")
        return youtube, youtubeAnalytics, credentials
    except Exception as e:
        logging.error(f"Failed to get credentials: {e}")
        raise

def get_video_analytics(youtubeAnalytics, video_id):
    end_date = datetime.today().strftime('%Y-%m-%d')
    request = youtubeAnalytics.reports().query(
        ids='channel==UCGq-a57w-aPwyi3pW7XLiHw',
        startDate='2022-01-01',  # Use a more recent start date
        endDate=end_date,
        metrics='relativeRetentionPerformance,audienceWatchRatio',
        dimensions='elapsedVideoTimeRatio',
        filters=f'video=={video_id}',
        sort='elapsedVideoTimeRatio'
    )
    try:
        logging.info(f"Requesting retention analytics for video ID: {video_id}")
        response = request.execute()
        return response
    except googleapiclient.errors.HttpError as e:
        logging.error(f"An error occurred: {e}")
        if e.resp.status == 403:
            logging.error("Access forbidden. Please check your API key, OAuth token, and permissions.")
        raise

def process_analytics_data(video_id, analytics_data):
    rows = analytics_data['rows']
    processed_data = []
    for row in rows:
        processed_data.append({
            'video_id': video_id,
            'elapsedVideoTimeRatio': row[0],
            'relativeRetentionPerformance': row[1],
            'audienceWatchRatio': row[2]
        })
    return processed_data

def append_to_csv(file_path, data):
    df = pd.DataFrame(data)
    df.to_csv(file_path, mode='a', header=not os.path.exists(file_path), index=False)

def append_to_bigquery(data):
    try:
        credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
        client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
        table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
        table = client.get_table(table_ref)
        errors = client.insert_rows_json(table, data)
        if errors:
            logging.error(f"Errors occurred while inserting rows to BigQuery: {errors}")
    except Exception as e:
        logging.error(f"Failed to append to BigQuery: {e}")
        raise

def main():
    try:
        youtube, youtubeAnalytics, credentials = get_credentials()
        
        # Directly specify the video ID
        video_id = 'cRjzQuzX-tg'
        
        try:
            analytics_data = get_video_analytics(youtubeAnalytics, video_id)
            processed_data = process_analytics_data(video_id, analytics_data)
            
            # Print the data to the console
            print(processed_data)
            
            # Append to CSV
            append_to_csv('retention_data.csv', processed_data)
            
            # Append to BigQuery
            append_to_bigquery(processed_data)
            
        except googleapiclient.errors.HttpError as e:
            logging.error(f"Failed to get analytics for video ID: {video_id}")
            # Re-authenticate if token is expired or invalid
            if e.resp.status in [401, 403]:
                logging.info("Re-authenticating due to invalid/expired token...")
                youtube, youtubeAnalytics, credentials = get_credentials()
                analytics_data = get_video_analytics(youtubeAnalytics, video_id)
                processed_data = process_analytics_data(video_id, analytics_data)
                
                # Print the data to the console
                print(processed_data)
                
                # Append to CSV
                append_to_csv('retention_data.csv', processed_data)
                
                # Append to BigQuery
                append_to_bigquery(processed_data)

        logging.info("Analytics data processing completed.")
    except Exception as e:
        logging.error(f"An error occurred in the main function: {e}")

if __name__ == "__main__":
    main()