import pandas as pd

# Read the CSV file
df = pd.read_csv('developer task youtube analytics data - task_data.csv')

# Create thumbnail URLs directly from video IDs
df['thumbnail_url'] = 'https://img.youtube.com/vi/' + df['episode_id'] + '/hqdefault.jpg'

# For maximum resolution (might not exist for all videos)
df['thumbnail_url_maxres'] = 'https://img.youtube.com/vi/' + df['episode_id'] + '/maxresdefault.jpg'

# Display the first few rows with the new column
print(df[['episode_id', 'episode_name', 'thumbnail_url']].head())

# Save the updated dataframe
df.to_csv('youtube_analytics_with_thumbnails.csv', index=False)

print(f"Added thumbnail URLs for {len(df)} videos and saved to youtube_analytics_with_thumbnails.csv")