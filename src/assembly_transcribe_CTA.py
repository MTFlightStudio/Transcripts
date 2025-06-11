import logging
import os
from dotenv import load_dotenv
import os
import time
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import assemblyai as aai
from openai import OpenAI
from google.cloud import bigquery

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv("config.env")

# Replace with your API keys
aai.settings.api_key = os.getenv("ASSEMBLYAI_API_KEY")
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Set the path to your service account key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "flightstudio-d8c6c3039d4c.json"

# Authenticate and create the PyDrive client
gauth = GoogleAuth()
gauth.LoadClientConfigFile("client_secrets.json")
gauth.LocalWebserverAuth()
drive = GoogleDrive(gauth)

# Create output directory if it doesn't exist
output_dir = os.path.join(os.getcwd(), "output")
os.makedirs(output_dir, exist_ok=True)

def download_from_gdrive(file_id, destination_file_name):
    logging.info(f"Downloading file with ID {file_id} from Google Drive to {destination_file_name}")
    file = drive.CreateFile({'id': file_id})
    file.GetContentFile(destination_file_name)
    logging.info(f"Downloaded file with ID {file_id} to {destination_file_name}")

def transcribe_audio(file_path, output_file):
    logging.info(f"Transcribing audio file {file_path}")
    config = aai.TranscriptionConfig(speaker_labels=False)
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
        return None, 0
    else:
        with open(output_file, "w") as file:
            file.write(transcript.text)
        logging.info(f"Transcription completed and saved to {output_file}")
        return transcript.text, len(transcript.text)

def summarize_transcript(transcript_text):
    logging.info("Generating summary for the transcript")
    prompt = (
        "Summarize the following transcript into succinct bullet points highlighting the main hooks/calls to action. "
        "Examples: 'Mentions podcast team', 'Prize Raffle', 'Thanks audience', 'Uses Statistics', 'Mentions Subscriber Count'.\n\n"
        f"{transcript_text}\n\n- "
    )
    
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": prompt}
        ],
        max_tokens=150,
        temperature=0.5
    )
    
    summary = response.choices[0].message.content.strip()
    logging.info("Summary generated")
    return summary

def video_id_exists_in_bigquery(video_id):
    client = bigquery.Client()
    query = f"""
        SELECT COUNT(*) as count
        FROM `flightstudio.youtube_transcript_data.CTA_transcripts`
        WHERE cta_video_id = @video_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("video_id", "STRING", video_id)
        ]
    )
    query_job = client.query(query, job_config=job_config)
    results = query_job.result()
    for row in results:
        if row.count > 0:
            return True
    return False

def upload_to_bigquery(video_id, transcript, summary):
    if video_id_exists_in_bigquery(video_id):
        logging.info(f"Video ID {video_id} already exists in BigQuery. Skipping upload.")
        return

    logging.info(f"Uploading transcript to BigQuery for video {video_id}")
    client = bigquery.Client()
    table_id = "flightstudio.youtube_transcript_data.CTA_transcripts"
    
    rows_to_insert = [
        {
            "cta_video_id": video_id,
            "cta_transcript": transcript,
            "cta_summary": summary,
            "cta_date_updated": time.strftime('%Y-%m-%d')  # Add date_updated
        }
    ]
    
    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors == []:
        logging.info(f"New rows have been added to {table_id}")
    else:
        logging.error(f"Encountered errors while inserting rows: {errors}")

def process_folder(folder_id):
    logging.info(f"Listing files in Google Drive folder {folder_id}")
    file_list = drive.ListFile({'q': f"'{folder_id}' in parents and trashed=false"}).GetList()

    for file in file_list:
        if file['title'].endswith((".mp4", ".m4v")):
            video_id = file['title'].replace('.mp4', '').replace('.m4v', '')
            transcription_file = os.path.join(output_dir, f"{video_id}_transcription.txt")
            
            if os.path.exists(transcription_file):
                logging.info(f"Transcription file {transcription_file} already exists. Skipping transcription.")
                with open(transcription_file, "r") as trans_file:
                    transcript_text = trans_file.read()
            else:
                logging.info(f"Processing file: {file['title']}")
                local_audio_file = os.path.join(output_dir, "temp_audio.mp4")
                download_from_gdrive(file['id'], local_audio_file)
                transcript_text, transcript_length = transcribe_audio(local_audio_file, transcription_file)
                os.remove(local_audio_file)  # Clean up local file
            
            summary = summarize_transcript(transcript_text)
            upload_to_bigquery(video_id, transcript_text, summary)
            logging.info(f"Completed processing for file: {file['title']}")

if __name__ == "__main__":
    folder_ids = ["1gag06lqpHtA27ttKZHUxse_rzrkRKUeu", "1bwkf6b5aDqbTqZ75_EY99OGN1Y40M4wH"]

    for folder_id in folder_ids:
        process_folder(folder_id)
    
    logging.info("All files processed")
