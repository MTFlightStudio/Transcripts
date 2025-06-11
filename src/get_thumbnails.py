import pandas as pd
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
import os
import pickle

# Function to get authenticated YouTube API service
def get_youtube_service():
    # Check if credentials file exists
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    else:
        # If no credentials, you'll need to use the authentication flow
        print("No credentials found. Please run authentication process first.")
        return None
    
    # Build the YouTube service
    return build('youtube', 'v3', credentials=creds)

# Function to get thumbnail URLs for a list of video IDs
def get_thumbnail_urls(youtube, video_ids):
    # Split video IDs into chunks of 50 (API limit)
    chunks = [video_ids[i:i + 50] for i in range(0, len(video_ids), 50)]
    
    thumbnail_dict = {}
    for chunk in chunks:
        # Make API request
        request = youtube.videos().list(
            part="snippet",
            id=','.join(chunk)
        )
        response = request.execute()
        
        # Extract thumbnail URLs
        for item in response['items']:
            video_id = item['id']
            # Get highest quality thumbnail available
            thumbnails = item['snippet']['thumbnails']
            if 'maxres' in thumbnails:
                thumbnail_url = thumbnails['maxres']['url']
            elif 'high' in thumbnails:
                thumbnail_url = thumbnails['high']['url']
            else:
                thumbnail_url = thumbnails['default']['url']
            
            thumbnail_dict[video_id] = thumbnail_url
    
    return thumbnail_dict

# Main function
def add_thumbnails_to_data():
    # Read the CSV file
    df = pd.read_csv('developer task youtube analytics data - task_data.csv')
    
    # Get YouTube service
    youtube = get_youtube_service()
    if not youtube:
        return None
    
    # Get unique video IDs
    video_ids = df['episode_id'].unique().tolist()
    
    # Get thumbnail URLs
    thumbnail_dict = get_thumbnail_urls(youtube, video_ids)
    
    # Add thumbnail URLs to dataframe
    df['thumbnail_url'] = df['episode_id'].map(thumbnail_dict)
    
    # Display the first few rows with the new column
    print(df[['episode_id', 'episode_name', 'thumbnail_url']].head())
    
    # Save the updated dataframe
    df.to_csv('youtube_analytics_with_thumbnails.csv', index=False)
    
    return df

# Run the script
if __name__ == "__main__":
    add_thumbnails_to_data()