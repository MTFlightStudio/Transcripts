import logging
from google.cloud import speech_v1p1beta1 as speech
from google.oauth2 import service_account
from google.cloud import bigquery, storage
from pydub import AudioSegment
import re
import time
import os
import random
from time import sleep

# Configure logging
logging.basicConfig(level=logging.INFO)

# Path to your service account key file
SERVICE_ACCOUNT_FILE = 'flightstudio-d8c6c3039d4c.json'

# Authenticate using the service account file
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE
)

# Initialize the clients
speech_client = speech.SpeechClient(credentials=credentials)
bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)
storage_client = storage.Client(credentials=credentials)

# Define the GCS bucket name and BigQuery table ID
bucket_name = "doac_youtube_transcripts"
table_id_full = f"{credentials.project_id}.youtube_transcript_data.transcripts"

# Define the BigQuery table schema
schema = [
    bigquery.SchemaField("episode_name", "STRING"),
    bigquery.SchemaField("youtube_id", "STRING"),
    bigquery.SchemaField("upload_date", "DATE"),
    bigquery.SchemaField("speaker_tag", "INTEGER"),
    bigquery.SchemaField("sentence", "STRING"),
    bigquery.SchemaField("start_time", "FLOAT64"),
    bigquery.SchemaField("end_time", "FLOAT64"),
]

# Fix the BigQuery table check with better logging
try:
    # Add project ID explicitly
    logging.info(f"Checking if table exists: {table_id_full}")
    
    table = bq_client.get_table(table_id_full)
    logging.info(f"Table exists with {table.num_rows} rows")
    
except Exception as e:
    logging.error(f"Error accessing BigQuery table: {e}")
    logging.error("Attempting to create table as fallback...")
    
    try:
        # Try to create the table
        dataset_id = f"{credentials.project_id}.youtube_transcript_data"
        
        # Check if dataset exists, create if not
        try:
            bq_client.get_dataset(dataset_id)
            logging.info(f"Dataset {dataset_id} exists")
        except Exception:
            logging.info(f"Creating dataset {dataset_id}")
            dataset = bigquery.Dataset(dataset_id)
            bq_client.create_dataset(dataset)
        
        # Create table
        table = bigquery.Table(table_id_full, schema=schema)
        bq_client.create_table(table)
        logging.info(f"Created table {table_id_full}")
    except Exception as create_error:
        logging.error(f"Failed to create table: {create_error}")
        raise

# Update all subsequent references to table_id to use table_id_full
table_id = table_id_full

# Get the GCS bucket and list of blobs (files)
bucket = storage_client.bucket(bucket_name)
blobs = list(bucket.list_blobs())

logging.info(f"Found {len(blobs)} files in the bucket.")

for i, blob in enumerate(blobs, start=1):
    gcs_uri = f"gs://{bucket_name}/{blob.name}"
    
    # IMPROVED: Extract episode name, YouTube ID, and upload date from filename
    # Pattern matches: Title_YouTubeID_Date.mp3
    match = re.search(r"(.+)_([A-Za-z0-9_-]{11})_(\d{8})\.(mp3|mov)", blob.name)
    if match:
        episode_name = match.group(1)
        youtube_id = match.group(2)
        upload_date = match.group(3)
        upload_date = f"{upload_date[:4]}-{upload_date[4:6]}-{upload_date[6:]}"
    else:
        logging.warning(f"GCS URI does not match the expected format: {gcs_uri}")
        continue

    logging.info(f"Processing file {i}/{len(blobs)}: {episode_name} ({youtube_id})")

    # Skip files that have already been processed
    # Use the existing BigQuery table to check
    query = f"""
    SELECT COUNT(*) as count
    FROM `{table_id}`
    WHERE episode_name = '{episode_name}' 
    AND youtube_id = '{youtube_id}'
    """
    
    try:
        query_job = bq_client.query(query)
        results = query_job.result()
        row = list(results)[0]
        if row.count > 0:
            logging.info(f"File {episode_name} ({youtube_id}) already processed. Skipping.")
            continue
    except Exception as e:
        logging.error(f"Error checking if file already processed: {e}")
        # Continue processing as a precaution
    
    # Download the audio file locally
    local_filename = f"temp_{youtube_id}.mp3"  # Use safer temp filename
    blob.download_to_filename(local_filename)
    audio = AudioSegment.from_file(local_filename)

    # Split the audio into 30-second chunks
    chunk_length_ms = 30 * 1000  # 30 seconds
    chunks = [audio[i:i + chunk_length_ms] for i in range(0, len(audio), chunk_length_ms)]

    for j, chunk in enumerate(chunks):
        chunk_filename = f"temp_{youtube_id}_chunk{j}.mp3"
        chunk.export(chunk_filename, format="mp3")

        # Upload the chunk to GCS
        chunk_blob = bucket.blob(f"chunks/{youtube_id}_chunk{j}.mp3")
        chunk_blob.upload_from_filename(chunk_filename)
        chunk_gcs_uri = f"gs://{bucket_name}/chunks/{youtube_id}_chunk{j}.mp3"

        # Configure the audio file reference
        audio = speech.RecognitionAudio(uri=chunk_gcs_uri)

        # Set up the speaker diarization config
        diarization_config = speech.SpeakerDiarizationConfig(
            enable_speaker_diarization=True,
            min_speaker_count=2,
            max_speaker_count=10,
        )

        # Set up the recognition config
        config = speech.RecognitionConfig(
            encoding=speech.RecognitionConfig.AudioEncoding.MP3,
            sample_rate_hertz=44100,
            language_code="en-US",
            diarization_config=diarization_config,
            enable_automatic_punctuation=True,  # Improved: add punctuation
            model="video",  # Better model for podcasts/interviews
        )

        logging.info(f"Initiating long-running operation for chunk {j+1}/{len(chunks)}...")
        start_time = time.time()
        operation = speech_client.long_running_recognize(config=config, audio=audio)
        logging.info(f"Operation initiated in {time.time() - start_time:.2f} seconds")

        # Wait for the operation to complete (with timeout handling)
        try:
            start_time = time.time()
            response = operation.result(timeout=3600)  # 1 hour timeout
            logging.info(f"Operation completed in {time.time() - start_time:.2f} seconds")
            
            if not response.results:
                logging.warning(f"No results found for chunk {j+1}. Skipping.")
                continue
        except Exception as e:
            logging.error(f"Operation failed: {e}")
            continue

        # Ensure the response structure is as expected
        if not response.results or not response.results[0].alternatives:
            logging.warning(f"Unexpected response structure. Skipping.")
            continue

        # Retrieve the last result (contains all word information)
        result = response.results[-1]
        words_info = result.alternatives[0].words

        # Prepare the transcript with speaker tags
        rows_to_insert = []
        current_speaker = None
        current_sentence = []
        start_time = None

        for word_info in words_info:
            if current_speaker is None:
                current_speaker = word_info.speaker_tag
                start_time = word_info.start_time.total_seconds()

            if word_info.speaker_tag != current_speaker:
                # New speaker, save the current sentence
                end_time = word_info.start_time.total_seconds()
                sentence = " ".join(current_sentence)
                rows_to_insert.append({
                    "episode_name": episode_name,
                    "youtube_id": youtube_id,
                    "upload_date": upload_date,
                    "speaker_tag": current_speaker,
                    "sentence": sentence,
                    "start_time": start_time,
                    "end_time": end_time
                })
                # Reset for the new speaker
                current_speaker = word_info.speaker_tag
                current_sentence = []
                start_time = word_info.start_time.total_seconds()

            current_sentence.append(word_info.word)

        # Add the last sentence
        if current_sentence:
            end_time = words_info[-1].end_time.total_seconds()
            sentence = " ".join(current_sentence)
            rows_to_insert.append({
                "episode_name": episode_name,
                "youtube_id": youtube_id,
                "upload_date": upload_date,
                "speaker_tag": current_speaker,
                "sentence": sentence,
                "start_time": start_time,
                "end_time": end_time
            })

        # Insert the data into BigQuery
        start_time = time.time()
        errors = bq_client.insert_rows_json(table_id, rows_to_insert)
        logging.info(f"Inserted {len(rows_to_insert)} rows into BigQuery in {time.time() - start_time:.2f} seconds")
        if not errors:
            logging.info(f"Successfully added {len(rows_to_insert)} rows.")
        else:
            logging.error(f"Encountered errors while inserting rows: {errors}")

        # Clean up local chunk file
        os.remove(chunk_filename)
        
        # Clean up GCS chunk file after processing
        chunk_blob.delete()
        logging.info(f"Deleted chunk from GCS: {chunk_gcs_uri}")

    # Clean up local original file
    os.remove(local_filename)
    
    logging.info(f"Completed processing file {i}/{len(blobs)}: {episode_name}")

if __name__ == "__main__":
    if not new_videos:
        logging.info("No new videos to download. All videos already exist in bucket.")
    else:
        logging.info(f"Found {len(new_videos)} new videos in new_videos.txt. Starting download and upload process.")
        
        # Use the 'new_videos.txt' as the source of IDs to process
        video_ids_to_process = load_video_ids('new_videos.txt') # load_video_ids can be reused

        if not video_ids_to_process:
            logging.info("new_videos.txt is empty or could not be read. No videos to process.")
        else:
            processed_count = 0
            failed_count = 0
            for video_id in video_ids_to_process:
                logging.info(f"Processing video ID: {video_id}")
                # Add a random delay between 30 and 120 seconds
                delay = random.randint(30, 120)
                logging.info(f"Waiting for {delay} seconds before next download...")
                sleep(delay)
                
                success = download_and_upload_to_gcs(video_id, bucket_name)
                if success:
                    processed_count += 1
                else:
                    failed_count += 1
                logging.info(f"Finished processing video ID: {video_id}. Success: {success}")

            logging.info(f"Download and upload process completed.")
            logging.info(f"Successfully processed {processed_count} videos.")
            if failed_count > 0:
                logging.warning(f"Failed to process {failed_count} videos. Check logs for details.")