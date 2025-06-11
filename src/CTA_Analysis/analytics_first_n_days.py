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

# Initialize logging
logging.basicConfig(level=logging.INFO)

# Path to your client secrets JSON file
CLIENT_SECRETS_FILE = 'client_secrets.json'
CHANNEL_ID = "UCh4iKqfMDE1TDBkgTidkOyg"

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

def get_youtube_service(credentials):
    youtube = build("youtube", "v3", credentials=credentials)
    return youtube

def get_youtube_analytics_service(credentials):
    youtube_analytics = build("youtubeAnalytics", "v2", credentials=credentials)
    return youtube_analytics

def get_video_ids(youtube, channel_id, published_after=None, published_before=None):
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
            logging.info(f"API response: {response}")

            for item in response['items']:
                video_id = item['id']['videoId']
                upload_date = item['snippet']['publishedAt']
                if upload_date > "2021-01-01T00:00:00Z":
                    videos.append(video_id)
                    logging.info(f"Found video ID: {video_id} with upload date: {upload_date}")

            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break
        except googleapiclient.errors.HttpError as e:
            logging.error(f"An error occurred: {e}")
            if e.resp.status == 403:
                logging.error("Access forbidden. Please check your API key, OAuth token, and permissions.")
            raise

    return videos

def get_video_upload_date(youtube, video_id):
    request = youtube.videos().list(
        part="snippet",
        id=video_id
    )
    response = request.execute()
    return response['items'][0]['snippet']['publishedAt']

def get_video_analytics(youtubeAnalytics, video_id, start_date, channel_id, days=5):
    # Convert start_date to datetime if it's a string
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
    
    # Calculate end_date (5 days after start_date)
    end_date = start_date + timedelta(days=days)
    
    # Format dates as strings for the API request
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    request = youtubeAnalytics.reports().query(
        ids=f'channel=={channel_id}',
        startDate=start_date_str,
        endDate=end_date_str,
        metrics='views,estimatedMinutesWatched,averageViewDuration,averageViewPercentage,subscribersGained,subscribersLost,likes,dislikes,comments,shares,estimatedRevenue',
        dimensions='video',
        filters=f'video=={video_id}',
        sort='video'
    )
    try:
        logging.info(f"Requesting analytics for video ID: {video_id} from {start_date_str} to {end_date_str}")
        response = request.execute()
        return response
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return None

def check_scopes(credentials):
    logging.info(f"Scopes: {credentials.scopes}")

def get_channel_details(youtube):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=CHANNEL_ID
    )
    try:
        response = request.execute()
        logging.info(response)
    except googleapiclient.errors.HttpError as e:
        logging.error(f"An error occurred: {e}")
        if e.resp.status == 403:
            logging.error("Access forbidden. Please check your API key, OAuth token, and permissions.")
        raise

def get_authenticated_channel_id(youtube):
    request = youtube.channels().list(
        part="id",
        mine=True
    )
    try:
        response = request.execute()
        channel_id = response['items'][0]['id']
        return channel_id
    except googleapiclient.errors.HttpError as e:
        logging.error(f"An error occurred: {e}")
        if e.resp.status == 403:
            logging.error("Access forbidden. Please check your API key, OAuth token, and permissions.")
        raise

def main():
    try:
        youtube, youtubeAnalytics, credentials = get_credentials()
        check_scopes(credentials)
        channel_id = get_authenticated_channel_id(youtube)
        get_channel_details(youtube)

        # Read the CSV file with video IDs and release dates
        df = pd.read_csv('youtube_cta_analysis.csv')
        
        # Convert release_date to datetime and filter for 2024 videos
        df['release_date'] = pd.to_datetime(df['release_date'])
        df_2024 = df[df['release_date'].dt.year == 2024]
        
        # Get analytics data for each 2024 video for the first 5 days
        all_analytics_data = []
        for _, row in df_2024.iterrows():
            video_id = row['episode_id']
            release_date = row['release_date']
            analytics_data = get_video_analytics(youtubeAnalytics, video_id, release_date, channel_id, days=5)
            if analytics_data:
                all_analytics_data.append(analytics_data)

        if all_analytics_data:
            # Extract rows and column headers
            rows = []
            for data in all_analytics_data:
                if 'rows' in data and data['rows']:
                    rows.extend(data['rows'])
            if rows:
                column_headers = [header['name'] for header in all_analytics_data[0]['columnHeaders']]
                
                # Rename 'video' column to 'video_id'
                column_headers = ['video_id' if col == 'video' else col for col in column_headers]
                
                # Create DataFrame
                results_df = pd.DataFrame(rows, columns=column_headers)
                
                # Merge with original DataFrame to include all columns
                final_df = pd.merge(df, results_df, left_on='episode_id', right_on='video_id', how='left')
                
                # Save the DataFrame to a CSV file
                output_file = 'video_analytics_data.csv'
                final_df.to_csv(output_file, index=False)
                logging.info(f"Video analytics data saved to {output_file}")
            else:
                logging.info("No rows found in the analytics data.")
        else:
            logging.info("No video analytics data to save.")
    except Exception as e:
        logging.error(f"An error occurred in the main function: {e}")

if __name__ == "__main__":
    main()