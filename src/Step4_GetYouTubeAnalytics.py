from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import googleapiclient.errors
import os
import logging
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account
import pickle
from google.auth.transport.requests import Request

# Initialize logging
logging.basicConfig(level=logging.INFO)

# Path to your client secrets JSON file
CLIENT_SECRETS_FILE = 'client_secrets.json'

# Scopes required for the YouTube API
SCOPES = [
    'https://www.googleapis.com/auth/youtube.readonly',
    'https://www.googleapis.com/auth/yt-analytics-monetary.readonly',
    'https://www.googleapis.com/auth/yt-analytics.readonly'
]

# Path to your service account key file
SERVICE_ACCOUNT_FILE = 'flightstudio-d8c6c3039d4c.json'

# BigQuery configuration
PROJECT_ID = 'flightstudio'
DATASET_ID = 'youtube_performance_data'
TABLE_ID = 'episode_analytics'

# Add channels dictionary at the top of the file
CHANNELS = {
    "UCGq-a57w-aPwyi3pW7XLiHw": "The Diary Of A CEO with Steven Bartlett",
    "UCzt4U1I0Wn-mWwZSOQp9lbQ": "We Need To Talk with Paul C. Brunson",
    "UCvYSNIBgdaYYgdwvWYBmckA": "Begin Again with Davina McCall"
}

def get_credentials(channel_id, channel_name):
    """Get credentials for specific channel"""
    token_file = f'token_{channel_id}.pickle'
    credentials = None
    
    # Load credentials from pickle file if available
    if os.path.exists(token_file):
        logging.info(f"Loading saved credentials from {token_file}")
        with open(token_file, 'rb') as token:
            credentials = pickle.load(token)
    
    # If no valid creds, run OAuth flow
    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            logging.info("Refreshing expired credentials")
            credentials.refresh(Request())
        else:
            print("\n" + "="*50)
            print(f"IMPORTANT: OAuth Flow for {channel_name}")
            print("="*50)
            print(f"When the browser opens:")
            print(f"1. Sign in with the Google account that has access to {channel_name}")
            print(f"2. If prompted to select a channel/brand account, select '{channel_name}'")
            print(f"3. Grant the requested permissions")
            print("="*50 + "\n")
            
            input(f"Press Enter to start OAuth flow for {channel_name}...")
            
            flow = InstalledAppFlow.from_client_secrets_file(
                'client_secrets.json',
                SCOPES
            )
            credentials = flow.run_local_server(port=0)
            
            # Save the credentials
            logging.info(f"Saving credentials to {token_file}")
            with open(token_file, 'wb') as token:
                pickle.dump(credentials, token)
    
    return credentials

def get_youtube_service(credentials):
    youtube = build("youtube", "v3", credentials=credentials)
    return youtube

def get_youtube_analytics_service(credentials):
    youtube_analytics = build("youtubeAnalytics", "v2", credentials=credentials)
    return youtube_analytics

def get_video_ids(youtube, channel_id, published_after=None, published_before=None, limit=None):
    videos = []
    next_page_token = None

    while True:
        request = youtube.search().list(
            part="id,snippet",
            channelId=channel_id,
            maxResults=50,
            type="video",
            publishedAfter=published_after,
            publishedBefore=published_before,
            pageToken=next_page_token
        )
        try:
            response = request.execute()
            #logging.info(f"API response: {response}")  # Log the API response for debugging

            for item in response['items']:
                video_id = item['id']['videoId']
                upload_date = item['snippet']['publishedAt']
                if upload_date > "2021-01-01T00:00:00Z":
                    videos.append(video_id)
                    logging.info(f"Found video ID: {video_id} with upload date: {upload_date}")  # Log each found video ID
                
                # Break if we've reached the desired limit
                if limit and len(videos) >= limit:
                    logging.info(f"Reached limit of {limit} videos, stopping search")
                    return videos

            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break
        except googleapiclient.errors.HttpError as e:
            logging.error(f"An error occurred: {e}")
            if e.resp.status == 403:
                logging.error("Access forbidden. Please check your API key, OAuth token, and permissions.")
            raise

    return videos

def get_video_analytics(youtubeAnalytics, video_id, start_date, channel_id):
    end_date = datetime.today().strftime('%Y-%m-%d')
    request = youtubeAnalytics.reports().query(
        ids=f'channel=={channel_id}',
        startDate=start_date,
        endDate=end_date,
        metrics='views,estimatedMinutesWatched,averageViewDuration,averageViewPercentage,subscribersGained,subscribersLost,likes,dislikes,comments,shares,estimatedRevenue',
        dimensions='video',
        filters=f'video=={video_id}',
        sort='video'
    )
    try:
        logging.info(f"Requesting analytics for video ID: {video_id} from channel: {channel_id}")
        response = request.execute()
        return response
    except googleapiclient.errors.HttpError as e:
        logging.error(f"An error occurred: {e}")
        raise

def get_video_details(youtube, video_id):
    try:
        request = youtube.videos().list(
            part="snippet",
            id=video_id
        )
        response = request.execute()
        if response['items']:
            snippet = response['items'][0]['snippet']
            return {
                'publish_date': snippet['publishedAt'],
                'title': snippet['title']
            }
        return None
    except googleapiclient.errors.HttpError as e:
        logging.error(f"Error getting video details: {e}")
        return None

def upload_to_bigquery(df, project_id, dataset_id, table_id):
    try:
        # Remove duplicates based on 'video_id'
        df = df.drop_duplicates(subset=['video_id'])
        
        # Add missing columns with None/0 values to match schema
        missing_columns = ['uniques', 'playlistViews', 'playlistStarts', 'viewsPerPlaylistStart', 
                         'cardClickRate', 'cardImpressions', 'cardClicks', 'monetizedPlaybacks', 
                         'adImpressions', 'cpm']
        
        for col in missing_columns:
            if col not in df.columns:
                # Use 0 for numeric columns
                df[col] = 0
        
        credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
        client = bigquery.Client(credentials=credentials, project=project_id)
        
        # Check if table exists, and get its schema if it does
        existing_table = None
        try:
            existing_table = client.get_table(f"{dataset_id}.{table_id}")
            existing_columns = [field.name for field in existing_table.schema]
            logging.info(f"Existing columns in BigQuery: {existing_columns}")
        except Exception as e:
            logging.warning(f"Table may not exist yet: {e}")
            existing_columns = []
        
        table_ref = client.dataset(dataset_id).table(table_id)
        temp_table_id = f"{table_id}_temp"
        temp_table_ref = client.dataset(dataset_id).table(temp_table_id)
        
        # Define schema for the temporary table
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("video_id", "STRING"),
                bigquery.SchemaField("views", "INTEGER"),
                bigquery.SchemaField("estimatedMinutesWatched", "INTEGER"),
                bigquery.SchemaField("averageViewDuration", "INTEGER"),
                bigquery.SchemaField("averageViewPercentage", "FLOAT"),
                bigquery.SchemaField("subscribersGained", "INTEGER"),
                bigquery.SchemaField("subscribersLost", "INTEGER"),
                bigquery.SchemaField("likes", "INTEGER"),
                bigquery.SchemaField("dislikes", "INTEGER"),
                bigquery.SchemaField("comments", "INTEGER"),
                bigquery.SchemaField("shares", "INTEGER"),
                bigquery.SchemaField("estimatedRevenue", "FLOAT"),
                bigquery.SchemaField("publish_date", "TIMESTAMP"),
                bigquery.SchemaField("video_title", "STRING"),
                bigquery.SchemaField("channel_name", "STRING"),
                # New metrics
                bigquery.SchemaField("uniques", "INTEGER"),
                bigquery.SchemaField("playlistViews", "INTEGER"),
                bigquery.SchemaField("playlistStarts", "INTEGER"),
                bigquery.SchemaField("viewsPerPlaylistStart", "FLOAT"),
                bigquery.SchemaField("cardClickRate", "FLOAT"),
                bigquery.SchemaField("cardImpressions", "INTEGER"),
                bigquery.SchemaField("cardClicks", "INTEGER"),
                bigquery.SchemaField("monetizedPlaybacks", "INTEGER"),
                bigquery.SchemaField("adImpressions", "INTEGER"),
                bigquery.SchemaField("cpm", "FLOAT"),
            ],
            write_disposition="WRITE_TRUNCATE",
        )
        
        job = client.load_table_from_dataframe(df, temp_table_ref, job_config=job_config)
        job.result()  # Wait for the job to complete
        
        # Build MERGE query with only the columns that exist in the table
        # If table doesn't exist yet, we still need to create it with the basic columns
        base_columns = ["video_id", "views", "estimatedMinutesWatched", "averageViewDuration", 
                        "averageViewPercentage", "subscribersGained", "subscribersLost", 
                        "likes", "dislikes", "comments", "shares", "estimatedRevenue",
                        "publish_date", "video_title", "channel_name"]
        
        # Build the update clause
        update_clause = ",\n            ".join([f"T.{col} = S.{col}" for col in base_columns if col != "video_id"])
        
        # Build the insert columns and values clauses
        insert_columns = "video_id, " + ", ".join([col for col in base_columns if col != "video_id"])
        insert_values = "S.video_id, " + ", ".join([f"S.{col}" for col in base_columns if col != "video_id"])
        
        merge_query = f"""
        MERGE `{project_id}.{dataset_id}.{table_id}` T
        USING `{project_id}.{dataset_id}.{temp_table_id}` S
        ON T.video_id = S.video_id
        WHEN MATCHED THEN
          UPDATE SET
            {update_clause}
        WHEN NOT MATCHED THEN
          INSERT ({insert_columns})
          VALUES ({insert_values})
        """
        
        client.query(merge_query).result()
        logging.info(f"Data successfully merged into BigQuery table {table_id}")
        
    except Exception as e:
        logging.error(f"Failed to upload to BigQuery: {e}")
        raise

def compare_video_ids(csv_file, project_id, dataset_id, table_id):
    # Read video IDs from CSV
    csv_df = pd.read_csv(csv_file)
    csv_video_ids = set(csv_df['video_id'])

    # Read video IDs from BigQuery
    credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
    client = bigquery.Client(credentials=credentials, project=project_id)
    query = f"SELECT video_id FROM `{project_id}.{dataset_id}.{table_id}`"
    bq_df = client.query(query).to_dataframe()
    bq_video_ids = set(bq_df['video_id'])

    # Compare video IDs
    missing_in_bq = csv_video_ids - bq_video_ids
    extra_in_bq = bq_video_ids - csv_video_ids

    logging.info(f"Video IDs in CSV but not in BigQuery: {missing_in_bq}")
    logging.info(f"Video IDs in BigQuery but not in CSV: {extra_in_bq}")

    return missing_in_bq, extra_in_bq

def main():
    try:
        all_data = []
        
        # Process each channel with its own OAuth credentials
        for channel_id, channel_name in CHANNELS.items():
            logging.info(f"\nProcessing channel: {channel_name}")
            
            # Get channel-specific credentials
            credentials = get_credentials(channel_id, channel_name)
            youtube = build('youtube', 'v3', credentials=credentials)
            youtubeAnalytics = build('youtubeAnalytics', 'v2', credentials=credentials)
            
            # Date range for the analytics data
            published_after = "2020-01-01T00:00:00Z"
            published_before = datetime.today().strftime('%Y-%m-%dT%H:%M:%SZ')

            # Get video IDs for this channel
            video_ids = get_video_ids(youtube, channel_id, published_after, published_before)
            
            # Get analytics data and video details for each video
            all_analytics_data = []
            video_details = {}
            channel_info = {}  # Store channel information for each video

            for video_id in video_ids:
                analytics_data = get_video_analytics(youtubeAnalytics, video_id, published_after.split("T")[0], channel_id)
                details = get_video_details(youtube, video_id)
                if analytics_data:
                    all_analytics_data.append(analytics_data)
                    if details:
                        video_details[video_id] = details
                        channel_info[video_id] = channel_name  # Store channel name for this video

            if all_analytics_data:
                # Extract rows and column headers
                rows = []
                for data in all_analytics_data:
                    rows.extend(data['rows'])
                column_headers = [header['name'] for header in all_analytics_data[0]['columnHeaders']]
                
                # Rename 'video' column to 'video_id'
                column_headers = ['video_id' if col == 'video' else col for col in column_headers]
                
                # Create DataFrame
                df = pd.DataFrame(rows, columns=column_headers)
                
                # Add publish_date, video_title, and channel_name columns
                df['publish_date'] = pd.to_datetime([video_details[vid]['publish_date'] if vid in video_details else None for vid in df['video_id']])
                df['video_title'] = [video_details[vid]['title'] if vid in video_details else None for vid in df['video_id']]
                df['channel_name'] = df['video_id'].map(channel_info)  # Add channel name
                
                # Save the DataFrame to a CSV file
                df.to_csv(f'video_analytics_data_{channel_name}.csv', index=False)
                logging.info(f"Video analytics data saved to video_analytics_data_{channel_name}.csv")
                
                # Update BigQuery schema and upload function
                upload_to_bigquery(df, PROJECT_ID, DATASET_ID, TABLE_ID)

                # Compare video IDs
                compare_video_ids(f'video_analytics_data_{channel_name}.csv', PROJECT_ID, DATASET_ID, TABLE_ID)
            else:
                logging.info("No video analytics data to save.")
    except Exception as e:
        logging.error(f"An error occurred in the main function: {e}")

if __name__ == "__main__":
    main()
