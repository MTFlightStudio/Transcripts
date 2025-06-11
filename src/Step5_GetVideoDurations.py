import os
import googleapiclient.discovery
from google.oauth2 import service_account
from google.cloud import bigquery
from datetime import datetime, timezone
import isodate
import time

# Path to your service account JSON key file
SERVICE_ACCOUNT_FILE = 'src/flightstudio-d8c6c3039d4c.json'  # Update to your JSON key file path

# Set the environment variable for Google Application Credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_FILE

# Channels to process
CHANNELS = {
    "UCGq-a57w-aPwyi3pW7XLiHw": "The Diary Of A CEO with Steven Bartlett",
    "UCzt4U1I0Wn-mWwZSOQp9lbQ": "We Need To Talk with Paul C. Brunson",
    "UCvYSNIBgdaYYgdwvWYBmckA": "Begin Again with Davina McCall"
}

# Initialize the YouTube API client using service account credentials
def get_youtube_service():
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=['https://www.googleapis.com/auth/youtube.readonly']
    )
    youtube = googleapiclient.discovery.build(
        "youtube", "v3", credentials=credentials)
    
    return youtube

# Retrieve video duration from YouTube API with retry logic
def get_video_duration(youtube, video_id, retries=3):
    for attempt in range(retries):
        try:
            request = youtube.videos().list(part='contentDetails', id=video_id)
            response = request.execute()
            if response['items']:
                duration = response['items'][0]['contentDetails']['duration']
                return duration
            return None
        except googleapiclient.errors.HttpError as e:
            if e.resp.status in [500, 503]:
                print(f"Server error ({e.resp.status}). Retrying...")
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                raise
    print(f"Failed to retrieve video duration for video ID {video_id} after {retries} attempts.")
    return None

# Convert ISO 8601 duration to seconds
def iso8601_to_seconds(duration):
    return isodate.parse_duration(duration).total_seconds()

# Retrieve all videos from a channel within a date range
def get_videos_from_channel(youtube, channel_id, published_after=None, published_before=None):
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

# Create BigQuery table and insert data
def create_bq_table_and_insert_data(video_data):
    client = bigquery.Client()
    table_id = "flightstudio.youtube_performance_data.episode_duration"

    schema = [
        bigquery.SchemaField("video_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("total_length", "FLOAT", mode="REQUIRED"),
    ]

    # Check if the table exists, and create it if it doesn't
    try:
        client.get_table(table_id)
    except:
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table)

    # Fetch existing video_ids from the table
    query = f"SELECT video_id FROM `{table_id}`"
    existing_video_ids = {row["video_id"] for row in client.query(query).result()}

    # Filter out video_data that already exists in the table
    new_video_data = {
        video_id: total_length
        for video_id, total_length in video_data.items()
        if video_id not in existing_video_ids
    }

    rows_to_insert = [
        {"video_id": video_id, "total_length": total_length}
        for video_id, total_length in new_video_data.items()
    ]

    if rows_to_insert:
        errors = client.insert_rows_json(table_id, rows_to_insert)
        if errors:
            print(f"Encountered errors while inserting rows: {errors}")
        else:
            print(f"Data successfully inserted into BigQuery table. {len(rows_to_insert)} rows added.")
    else:
        print("No new rows to add.")

def main():
    # Use a consistent start date
    published_after = "2020-01-01T00:00:00Z"
    published_before = datetime.now(timezone.utc).isoformat("T")

    youtube = get_youtube_service()
    
    all_video_data = {}
    
    # Loop through all channels
    for channel_id, channel_name in CHANNELS.items():
        print(f"\nProcessing channel: {channel_name} ({channel_id})")
        
        # Get all videos for the current channel
        videos = get_videos_from_channel(youtube, channel_id, published_after, published_before)
        print(f"Found {len(videos)} videos for {channel_name}.")
    
        # Get duration for each video
    for video_id in videos:
        duration = get_video_duration(youtube, video_id)
        if duration:
            total_length = iso8601_to_seconds(duration)
                all_video_data[video_id] = total_length

    print(f"\nFound total duration for {len(all_video_data)} videos across all channels.")
    create_bq_table_and_insert_data(all_video_data)

if __name__ == "__main__":
    main()