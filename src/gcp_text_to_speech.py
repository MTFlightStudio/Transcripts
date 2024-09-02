import logging
from google.cloud import speech_v1p1beta1 as speech
from google.oauth2 import service_account
from google.cloud import bigquery, storage
from pydub import AudioSegment
import re
import time
import os

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
table_id = "flightstudio.youtube_transcript_data.transcripts"

# Define the BigQuery table schema
schema = [
    bigquery.SchemaField("episode_name", "STRING"),
    bigquery.SchemaField("upload_date", "DATE"),
    bigquery.SchemaField("speaker_tag", "INTEGER"),
    bigquery.SchemaField("sentence", "STRING"),
    bigquery.SchemaField("start_time", "FLOAT64"),
    bigquery.SchemaField("end_time", "FLOAT64"),
]

# Check if the table exists and create it if it does not
try:
    bq_client.get_table(table_id)
except bigquery.NotFound:
    table = bigquery.Table(table_id, schema=schema)
    bq_client.create_table(table)
    logging.info(f"Created table {table_id}.")
except Exception as e:
    logging.error(f"Failed to check or create table: {e}")
    raise

# Get the GCS bucket and list of blobs (files)
bucket = storage_client.bucket(bucket_name)
blobs = list(bucket.list_blobs())

logging.info(f"Found {len(blobs)} files in the bucket.")

for i, blob in enumerate(blobs, start=1):
    gcs_uri = f"gs://{bucket_name}/{blob.name}"
    
    # Extract episode name and upload date from the GCS URI
    match = re.search(r"(.+)_(\d{8})\.(mp3|mov)", blob.name)
    if match:
        episode_name = match.group(1)
        upload_date = match.group(2)
        upload_date = f"{upload_date[:4]}-{upload_date[4:6]}-{upload_date[6:]}"
    else:
        logging.warning(f"GCS URI does not match the expected format: {gcs_uri}")
        continue

    logging.info(f"Processing file {i}/{len(blobs)}: {gcs_uri}")

    # Download the audio file locally
    blob.download_to_filename(blob.name)
    audio = AudioSegment.from_file(blob.name)

    # Split the audio into 30-second chunks
    chunk_length_ms = 30 * 1000  # 30 seconds
    chunks = [audio[i:i + chunk_length_ms] for i in range(0, len(audio), chunk_length_ms)]

    for j, chunk in enumerate(chunks):
        chunk_filename = f"{blob.name}_chunk{j}.mp3"
        chunk.export(chunk_filename, format="mp3")

        # Upload the chunk to GCS
        chunk_blob = bucket.blob(chunk_filename)
        chunk_blob.upload_from_filename(chunk_filename)
        chunk_gcs_uri = f"gs://{bucket_name}/{chunk_filename}"

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
            encoding=speech.RecognitionConfig.AudioEncoding.MP3,  # Adjust if your file is not in MP3 format
            sample_rate_hertz=44100,  # You can adjust this based on your file's properties
            language_code="en-US",
            diarization_config=diarization_config,
        )

        logging.info(f"Initiating long-running operation for {chunk_gcs_uri}...")
        start_time = time.time()
        operation = speech_client.long_running_recognize(config=config, audio=audio)
        logging.info(f"Long-running operation initiated for {chunk_gcs_uri} in {time.time() - start_time:.2f} seconds")

        # Wait for the operation to complete (with timeout handling)
        try:
            start_time = time.time()
            response = operation.result(timeout=3600)  # Adjust the timeout based on your needs
            logging.info(f"Operation completed for {chunk_gcs_uri} in {time.time() - start_time:.2f} seconds")
            
            # Debugging: Log the response
            logging.debug(f"API response: {response}")
            
            if not response.results:
                logging.warning(f"No results found for {chunk_gcs_uri}. Skipping.")
                continue
        except Exception as e:
            logging.error(f"Operation failed for {chunk_gcs_uri}: {e}")
            continue

        # Ensure the response structure is as expected
        if not response.results or not response.results[0].alternatives:
            logging.warning(f"Unexpected response structure for {chunk_gcs_uri}. Skipping.")
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
                "upload_date": upload_date,
                "speaker_tag": current_speaker,
                "sentence": sentence,
                "start_time": start_time,
                "end_time": end_time
            })

        # Insert the data into BigQuery
        start_time = time.time()
        errors = bq_client.insert_rows_json(table_id, rows_to_insert)
        logging.info(f"Inserted {len(rows_to_insert)} rows into BigQuery for {chunk_gcs_uri} in {time.time() - start_time:.2f} seconds")
        if not errors:
            logging.info(f"New rows have been added for {chunk_gcs_uri}.")
        else:
            logging.error(f"Encountered errors while inserting rows for {chunk_gcs_uri}: {errors}")

        # Clean up local chunk file
        os.remove(chunk_filename)

    # Clean up local original file
    os.remove(blob.name)