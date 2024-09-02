import logging
import os
import time
from google.cloud import bigquery

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def upload_to_bigquery(episode_id, episode_name, release_date, labeled_transcript, transcript_length):
    logging.info(f"Uploading labeled transcript to BigQuery for episode {episode_id}")
    client = bigquery.Client()
    table_id = "flightstudio.youtube_transcript_data.podcast_transcripts"
    
    rows_to_insert = [
        {
            "episode_id": episode_id,
            "episode_name": episode_name,
            "release_date": release_date,
            "transcript": labeled_transcript,
            "transcript_length": transcript_length,
            "transcribed_time": time.strftime('%Y-%m-%d %H:%M:%S')  # Add transcribed_time
        }
    ]
    
    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors == []:
        logging.info(f"New rows have been added to {table_id}")
    else:
        logging.error(f"Encountered errors while inserting rows: {errors}")

def get_existing_episode_ids():
    client = bigquery.Client()
    query = """
        SELECT episode_id
        FROM `flightstudio.youtube_transcript_data.podcast_transcripts`
    """
    query_job = client.query(query)
    results = query_job.result()
    return {row.episode_id for row in results}

if __name__ == "__main__":
    # Set the path to your service account key file
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "flightstudio-d8c6c3039d4c.json"

    # Get existing episode IDs from BigQuery
    existing_episode_ids = get_existing_episode_ids()

    # Define the transcription file details
    transcription_file = "Codie Sanchez: They're Lying To You About How To Get Rich! How To Turn $0 Into $1M!_IYu_PDPqKFc_20240812_transcription_labeled.txt"
    episode_id = "IYu_PDPqKFc"
    episode_name = "Codie Sanchez: They're Lying To You About How To Get Rich! How To Turn $0 Into $1M!"
    release_date = "2024-08-12"

    # Check if the episode ID already exists in BigQuery
    if episode_id in existing_episode_ids:
        logging.info(f"Skipping {transcription_file} as it already exists in BigQuery")
    else:
        # Read labeled transcript content
        with open(transcription_file, "r") as file:
            labeled_transcript = file.read()
        
        transcript_length = len(labeled_transcript)
        
        # Upload to BigQuery
        upload_to_bigquery(episode_id, episode_name, release_date, labeled_transcript, transcript_length)
        logging.info(f"Completed uploading for file: {transcription_file}")