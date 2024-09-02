import sys
print(sys.path)  # Print the sys.path to debug the import issue
from dotenv import load_dotenv
import os
import assemblyai as aai  # Ensure this package is installed and there are no local naming conflicts
from openai import OpenAI
from google.cloud import storage, bigquery
from googleapiclient.discovery import build  # Add this import for YouTube API
import os
import logging
import time
import re
from google.oauth2 import service_account  # Add this import for service account credentials
import googleapiclient.discovery  # Add this import statement

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

 # Load environment variables from the config.env file
load_dotenv("config.env")

# Replace with your API keys
aai.settings.api_key = os.getenv("ASSEMBLYAI_API_KEY")
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
# Set the path to your service account key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "flightstudio-d8c6c3039d4c.json"

def download_from_gcs(bucket_name, source_blob_name, destination_file_name):
    logging.info(f"Downloading {source_blob_name} from bucket {bucket_name} to {destination_file_name}")
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)
    logging.info(f"Downloaded {source_blob_name} from bucket {bucket_name} to {destination_file_name}")

def transcribe_audio(file_path, output_file):
    logging.info(f"Transcribing audio file {file_path}")
    config = aai.TranscriptionConfig(
        speaker_labels=True,
        #speakers_expected=2  # Set the number of expected speakers to 2
    )
    transcriber = aai.Transcriber()
    transcript = transcriber.transcribe(file_path, config=config)

    # Add timeout mechanism
    timeout = 600  # Timeout in seconds (10 minutes)
    start_time = time.time()
    last_log_time = start_time

    while transcript.status in [aai.TranscriptStatus.queued, aai.TranscriptStatus.processing]:
        current_time = time.time()
        if current_time - start_time > timeout:
            logging.error("Transcription timed out.")
            return None, 0
        if current_time - last_log_time >= 30:
            logging.info("Polling for transcription status...")
            last_log_time = current_time
        time.sleep(5)  # Polling interval

    if transcript.status == aai.TranscriptStatus.error:
        logging.error(f"Transcription error: {transcript.error}")
    else:
        with open(output_file, "w") as file:
            for utterance in transcript.utterances:
                start_time = utterance.start / 1000  # Convert milliseconds to seconds
                end_time = utterance.end / 1000  # Convert milliseconds to seconds
                file.write(f"Speaker {utterance.speaker}: {utterance.text} [{start_time:.2f}-{end_time:.2f}]\n")
        logging.info(f"Transcription completed and saved to {output_file}")
        return transcript.text, len(transcript.text)

def identify_interviewer(transcription_file):
    logging.info(f"Identifying interviewer in transcription file {transcription_file}")
    with open(transcription_file, "r") as file:
        lines = file.readlines()

    # Skip the first 15 lines to avoid the trailer
    lines = lines[15:]

    # Extract meaningful question lines
    question_lines = [line.strip() for line in lines if '?' in line and len(line.strip()) > 5]
    question_lines.sort(key=len)
    
    # Handle case where there are not enough question lines
    if len(question_lines) < 10:
        logging.warning(f"Not enough question lines found in {transcription_file}")
        return None

    # Create snippet by considering some context (lines before and after)
    snippet = "".join(question_lines[:20])

    # Add a unique identifier to the prompt to avoid caching issues
    unique_id = time.time()

    prompt = f"""
    The following is a snippet of an interview. The interviewer is always Steven Bartlett. Identify if Steven Bartlett is Speaker A or Speaker B. The interviewer typically asks questions and the interviewee provides answers.

    {snippet}

    Answer with only 'Speaker A' or 'Speaker B':
    (Unique ID: {unique_id})
    """

    response = client.chat.completions.create(
        model="gpt-4o",  # Use the GPT-4o model
        messages=[
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": prompt}
        ],
        max_tokens=10,  # Limit the response length
        temperature=0.5
    )

    identified_speaker = response.choices[0].message.content.strip().split('\n')[0]
    logging.info(f"Identified interviewer as: {identified_speaker}")
    return identified_speaker

def label_transcription(transcription_file, labeled_file, identified_speaker):
    logging.info(f"Labeling transcription file {transcription_file} with identified speaker {identified_speaker}")
    with open(transcription_file, "r") as file:
        lines = file.readlines()

    with open(labeled_file, "w") as file:   
        for line in lines:
            speaker = line.split(":")[0].strip()
            text = line.split(":", 1)[1].strip()
            if speaker == identified_speaker:
                file.write(f"Steven Bartlett: {text}\n")
            else:
                file.write(f"Interviewee: {text}\n")
    logging.info(f"Labeled transcription saved to {labeled_file}")

def upload_to_bigquery(episode_id, episode_name, release_date, labeled_transcript, transcript_length, guest_name, episode_description):
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
            "transcribed_time": time.strftime('%Y-%m-%d %H:%M:%S'),  # Add transcribed_time
            "guest_name": guest_name,
            "episode_description": episode_description
        }
    ]
    
    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors == []:
        logging.info(f"New rows have been added to {table_id}")
    else:
        logging.error(f"Encountered errors while inserting rows: {errors}")
        for error in errors:
            logging.error(f"Error details: {error}")

def get_existing_episode_ids():
    client = bigquery.Client()
    query = """
        SELECT episode_id
        FROM `flightstudio.youtube_transcript_data.podcast_transcripts`
    """
    query_job = client.query(query)
    results = query_job.result()
    return {row.episode_id for row in results}

def extract_guest_name(title, description):
    # Try to extract from title first
    if "with" in title:
        return title.split("with")[1].strip()
    
    # Fallback to extracting from description
    match = re.search(r'([A-Z][a-z]+ [A-Z][a-z]+)', description)
    if match:
        return match.group(1)
    
    return None

def get_youtube_service():
    SERVICE_ACCOUNT_FILE = "flightstudio-d8c6c3039d4c.json" # Update with your service account file path
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=['https://www.googleapis.com/auth/youtube.readonly']
    )
    youtube = googleapiclient.discovery.build(
        "youtube", "v3", credentials=credentials)
    
    return youtube

def get_youtube_video_info(youtube, video_id):
    request = youtube.videos().list(part='snippet', id=video_id)
    response = request.execute()
    if response['items']:
        snippet = response['items'][0]['snippet']
        return snippet['description'], snippet['title']
    return None, None

if __name__ == "__main__":
    bucket_name = "doac_youtube_transcripts"  # Replace with your GCS bucket name
    local_audio_file = "temp_audio.mp3"

    # Get existing episode IDs from BigQuery
    existing_episode_ids = get_existing_episode_ids()

    # List files in the GCS bucket
    logging.info(f"Listing files in GCS bucket {bucket_name}")
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = list(bucket.list_blobs())

    # Extract upload date from blob name and sort by it in descending order
    def extract_upload_date(blob_name):
        match = re.match(r".+_(\d{8})\.mp3", blob_name)
        if match:
            return match.group(1)
        return "00000000"  # Default to an early date if no match

    blobs.sort(key=lambda x: extract_upload_date(x.name), reverse=True)

    for blob in blobs:
        if blob.name.endswith(".mp3"):
            base_name = blob.name.replace('.mp3', '')
            
            # Extract episode details from the blob name
            match = re.match(r"(.+)_([A-Za-z0-9_-]{11})_(\d{8})", base_name)
            if match:
                video_title, video_id, upload_date = match.groups()
                
                # Skip processing if episode_id already exists in BigQuery
                if video_id in existing_episode_ids:
                    logging.info(f"Skipping {blob.name} as it already exists in BigQuery")
                    continue
                
                logging.info(f"Processing blob: {blob.name}")
                download_from_gcs(bucket_name, blob.name, local_audio_file)
                
                transcription_file = f"{base_name}_transcription.txt"
                labeled_file = f"{base_name}_transcription_labeled.txt"
                
                # Check if transcription file already exists
                if os.path.exists(transcription_file):
                    logging.info(f"Transcription file {transcription_file} already exists. Skipping transcription.")
                    with open(transcription_file, "r") as file:
                        transcript_text = file.read()
                    transcript_length = len(transcript_text)
                else:
                    transcript_text, transcript_length = transcribe_audio(local_audio_file, transcription_file)
                
                identified_speaker = identify_interviewer(transcription_file)
                label_transcription(transcription_file, labeled_file, identified_speaker)
                
                release_date = f"{upload_date[:4]}-{upload_date[4:6]}-{upload_date[6:]}"
                
                # Read labeled transcript content
                with open(labeled_file, "r") as file:
                    labeled_transcript = file.read()
                
                # Get guest name and episode description
                youtube = get_youtube_service()
                description, title = get_youtube_video_info(youtube, video_id)
                guest_name = extract_guest_name(title, description)
                
                # Upload to BigQuery
                upload_to_bigquery(video_id, video_title, release_date, labeled_transcript, transcript_length, guest_name, description)
                
                os.remove(local_audio_file)  # Clean up local file
                logging.info(f"Completed processing for blob: {blob.name}")
            else:
                logging.error(f"Failed to parse blob name: {blob.name}")
    logging.info("All files processed")
