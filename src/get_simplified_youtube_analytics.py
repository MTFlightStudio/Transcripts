from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import googleapiclient.errors
import os
import logging
import pandas as pd
from datetime import datetime
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

# Add channels dictionary
CHANNELS = {
    "UCGq-a57w-aPwyi3pW7XLiHw": "The Diary Of A CEO with Steven Bartlett",

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
                CLIENT_SECRETS_FILE,
                SCOPES
            )
            credentials = flow.run_local_server(port=0)
            
            # Save the credentials
            logging.info(f"Saving credentials to {token_file}")
            with open(token_file, 'wb') as token:
                pickle.dump(credentials, token)
    
    return credentials

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
            for item in response['items']:
                video_id = item['id']['videoId']
                upload_date = item['snippet']['publishedAt']
                videos.append(video_id)
                logging.info(f"Found video ID: {video_id} with upload date: {upload_date}")

            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break
        except googleapiclient.errors.HttpError as e:
            logging.error(f"An error occurred: {e}")
            raise

    return videos

def get_video_analytics(youtubeAnalytics, video_id, start_date, channel_id):
    end_date = datetime.today().strftime('%Y-%m-%d')
    # Only requesting views and subscribersGained
    request = youtubeAnalytics.reports().query(
        ids=f'channel=={channel_id}',
        startDate=start_date,
        endDate=end_date,
        metrics='views,subscribersGained',
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

def main():
    try:
        # Create a master dataframe to hold all channel data
        master_df = pd.DataFrame()
        
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
                if analytics_data and 'rows' in analytics_data and analytics_data['rows']:
                    all_analytics_data.append(analytics_data)
                    if details:
                        video_details[video_id] = details
                        channel_info[video_id] = channel_name

            if all_analytics_data:
                # Extract rows and column headers
                rows = []
                for data in all_analytics_data:
                    if 'rows' in data:
                        rows.extend(data['rows'])
                column_headers = [header['name'] for header in all_analytics_data[0]['columnHeaders']]
                
                # Rename 'video' column to 'video_id'
                column_headers = ['video_id' if col == 'video' else col for col in column_headers]
                
                # Create DataFrame
                df = pd.DataFrame(rows, columns=column_headers)
                
                # Add publish_date, video_title, and channel_name columns
                df['publish_date'] = pd.to_datetime([video_details[vid]['publish_date'] if vid in video_details else None for vid in df['video_id']])
                df['video_title'] = [video_details[vid]['title'] if vid in video_details else None for vid in df['video_id']]
                df['channel_name'] = df['video_id'].map(channel_info)
                
                # Append to master dataframe
                master_df = pd.concat([master_df, df], ignore_index=True)
                
                # Also save individual channel data
                df.to_csv(f'simplified_analytics_{channel_name}.csv', index=False)
                logging.info(f"Channel data saved to simplified_analytics_{channel_name}.csv")
            else:
                logging.info(f"No video analytics data for {channel_name}.")
        
        # Save the combined data
        if not master_df.empty:
            master_df.to_csv('all_channels_views_and_subscribers.csv', index=False)
            logging.info("Combined data from all channels saved to all_channels_views_and_subscribers.csv")
            
            # Display summary to console
            print("\n===== SUMMARY OF VIEWS AND SUBSCRIBERS GAINED =====")
            summary = master_df.sort_values(by='views', ascending=False)
            pd.set_option('display.max_rows', None)  # Show all rows
            print(summary[['video_title', 'views', 'subscribersGained', 'channel_name']])
        else:
            logging.info("No data collected from any channel.")
            
    except Exception as e:
        logging.error(f"An error occurred in the main function: {e}")

if __name__ == "__main__":
    main() 