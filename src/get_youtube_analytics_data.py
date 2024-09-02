from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import googleapiclient.errors
import os
import logging
import pandas as pd
from datetime import datetime

# Initialize logging
logging.basicConfig(level=logging.INFO)

# Path to your client secrets JSON file
CLIENT_SECRETS_FILE = 'client_secrets_2.json'
CHANNEL_ID = "UCGq-a57w-aPwyi3pW7XLiHw"

# Scopes required for the YouTube API
SCOPES = [
    'https://www.googleapis.com/auth/youtube.readonly',
    'https://www.googleapis.com/auth/yt-analytics.readonly'
]

def get_credentials():
    try:
        logging.info("Starting OAuth flow...")
        flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
        flow.redirect_uri = 'http://localhost:50517/signin-google'  # Update to match the new redirect URI
        logging.info(f"Redirect URI: {flow.redirect_uri}")
        credentials = flow.run_local_server(port=50517)  # Ensure this matches the registered redirect URI
        logging.info("OAuth flow completed successfully.")
        return credentials
    except Exception as e:
        logging.error(f"Failed to get credentials: {e}")
        raise

def get_youtube_service(credentials):
    youtube = build("youtube", "v3", credentials=credentials)
    return youtube

def get_youtube_analytics_service(credentials):
    youtube_analytics = build("youtubeAnalytics", "v2", credentials=credentials)
    return youtube_analytics

def get_video_ids(credentials):
    youtube = get_youtube_service(credentials)
    request = youtube.search().list(
        part="id",
        channelId=CHANNEL_ID,
        maxResults=50,
        type="video"
    )
    response = request.execute()

    video_ids = [item['id']['videoId'] for item in response['items']]
    return video_ids

def get_video_analytics(credentials, video_id):
    youtubeAnalytics = get_youtube_analytics_service(credentials)
    end_date = datetime.today().strftime('%Y-%m-%d')
    request = youtubeAnalytics.reports().query(
        ids='channel==UCGq-a57w-aPwyi3pW7XLiHw',
        startDate='2022-01-01',  # Use a more recent start date
        endDate=end_date,
        metrics='views',
        dimensions='day',
        filters=f'video=={video_id}',
        sort='day'
    )
    try:
        logging.info(f"Requesting analytics for video ID: {video_id}")
        response = request.execute()
        return response
    except googleapiclient.errors.HttpError as e:
        logging.error(f"An error occurred: {e}")
        if e.resp.status == 403:
            logging.error("Access forbidden. Please check your API key, OAuth token, and permissions.")
        raise

def check_scopes(credentials):
    print("Scopes:", credentials.scopes)

def get_channel_details(credentials):
    youtube = get_youtube_service(credentials)
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=CHANNEL_ID
    )
    try:
        response = request.execute()
        print(response)
    except googleapiclient.errors.HttpError as e:
        print(f"An error occurred: {e}")
        if e.resp.status == 403:
            print("Access forbidden. Please check your API key, OAuth token, and permissions.")
        raise

def main():
    try:
        credentials = get_credentials()
        check_scopes(credentials)  # Add this line to check scopes
        get_channel_details(credentials)  # Add this line to check channel details
        video_ids = get_video_ids(credentials)
        all_analytics_data = []

        for video_id in video_ids:
            try:
                analytics_data = get_video_analytics(credentials, video_id)
                all_analytics_data.append(analytics_data)
            except googleapiclient.errors.HttpError as e:
                logging.error(f"Failed to get analytics for video ID: {video_id}")

        # Convert the data to a DataFrame
        df = pd.json_normalize(all_analytics_data)
        
        # Save the DataFrame to a CSV file
        df.to_csv('all_analytics_data.csv', index=False)
    except Exception as e:
        logging.error(f"An error occurred in the main function: {e}")

if __name__ == "__main__":
    main()