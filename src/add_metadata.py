import logging
import os
import re
import time
from google.cloud import bigquery
from googleapiclient.discovery import build

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_youtube_video_info(api_key, video_id):
    youtube = build('youtube', 'v3', developerKey=api_key)
    request = youtube.videos().list(part='snippet', id=video_id)
    response = request.execute()
    if response['items']:
        snippet = response['items'][0]['snippet']
        return snippet['description'], snippet['title']
    return None, None

def extract_guest_name(title, description):
    # Try to extract from title first
    if "with" in title:
        return title.split("with")[1].strip()
    
    # Fallback to extracting from description
    match = re.search(r'([A-Z][a-z]+ [A-Z][a-z]+)', description)
    if match:
        return match.group(1)
    
    return None

def update_episode_info(client, table_id, episode_id, description, guest_name):
    temp_table_id = f"{table_id}_temp"

    query_create_temp = f"""
        CREATE OR REPLACE TABLE `{temp_table_id}` AS
        SELECT 
            episode_id,
            episode_name,
            release_date,
            transcript,
            transcript_length,
            transcribed_time,
            @guest_name AS guest_name,
            @description AS episode_description
        FROM `{table_id}`
        WHERE episode_id = @episode_id
    """
    job_config_create_temp = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("description", "STRING", description),
            bigquery.ScalarQueryParameter("guest_name", "STRING", guest_name),
            bigquery.ScalarQueryParameter("episode_id", "STRING", episode_id),
        ]
    )
    client.query(query_create_temp, job_config=job_config_create_temp).result()

    query_copy_back = f"""
        INSERT INTO `{table_id}`
        SELECT * FROM `{temp_table_id}`
    """
    client.query(query_copy_back).result()

    client.delete_table(temp_table_id, not_found_ok=True)

    logging.info(f"Updated episode info for episode_id: {episode_id}")

def add_columns_if_not_exist(client, table_id):
    table = client.get_table(table_id)
    existing_fields = [field.name for field in table.schema]

    new_fields = []
    if 'episode_description' not in existing_fields:
        new_fields.append(bigquery.SchemaField('episode_description', 'STRING'))
    if 'guest_name' not in existing_fields:
        new_fields.append(bigquery.SchemaField('guest_name', 'STRING'))

    if new_fields:
        table.schema = table.schema + new_fields
        client.update_table(table, ["schema"])
        logging.info(f"Added new columns to {table_id}: {[field.name for field in new_fields]}")

def process_episodes():
    client = bigquery.Client()
    table_id = "flightstudio.youtube_transcript_data.podcast_transcripts"
    api_key = os.getenv("YOUTUBE_API_KEY")

    add_columns_if_not_exist(client, table_id)

    query = f"""
        SELECT episode_id
        FROM `{table_id}`
        WHERE episode_description IS NULL OR guest_name IS NULL
    """
    query_job = client.query(query)
    results = query_job.result()

    for row in results:
        episode_id = row.episode_id

        description, title = get_youtube_video_info(api_key, episode_id)
        if description and title:
            guest_name = extract_guest_name(title, description)
            update_episode_info(client, table_id, episode_id, description, guest_name)

if __name__ == "__main__":
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "flightstudio-d8c6c3039d4c.json"
    process_episodes()
