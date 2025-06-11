import os
import googleapiclient.discovery
from google.oauth2 import service_account
import tempfile
from datetime import datetime

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

# Retrieve all videos from a channel within a date range
def get_videos_from_channel(channel_id, published_after=None, published_before=None):
    youtube = get_youtube_service()
    videos = []
    next_page_token = None

    while True:
        request = youtube.search().list(
            part="snippet",
            channelId=channel_id,
            maxResults=50,  # Maximum allowed by the API
            order="date",
            type="video",
            publishedAfter=published_after,
            publishedBefore=published_before,
            pageToken=next_page_token
        )
        response = request.execute()

        for item in response['items']:
            video_id = item['id']['videoId']
            videos.append(video_id)

        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break
    
    return videos

# Store video IDs to a file
def store_video_ids_to_file(video_ids, file_path='video_ids.txt'):
    with open(file_path, 'w') as file:
        for video_id in video_ids:
            file.write(f"{video_id}\n")
    return file_path

channel_id = "UCGq-a57w-aPwyi3pW7XLiHw"  # Replace with your YouTube channel ID
published_after = "2021-08-22T00:00:00Z"  # Replace with your start date
published_before =   datetime.utcnow().isoformat("T") + "Z"  # Use today's date as the end date

videos = get_videos_from_channel(channel_id, published_after, published_before)
file_path = store_video_ids_to_file(videos)
print(f"Video IDs stored in file: {file_path}")