import logging
import os
import time  # Add this import
from google.cloud import bigquery

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def update_transcript_labels(client, table_id, episode_id, new_transcript):
    temp_table_id = f"{table_id}_temp"

    # Create a temporary table with the updated data
    query_create_temp = f"""
        CREATE OR REPLACE TABLE `{temp_table_id}` AS
        SELECT 
            episode_id,
            episode_name,
            release_date,
            CAST(@new_transcript AS STRING) AS transcript,
            transcript_length,
            @transcribed_time AS transcribed_time
        FROM `{table_id}`
        WHERE episode_id = @episode_id
    """
    job_config_create_temp = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("new_transcript", "STRING", new_transcript),
            bigquery.ScalarQueryParameter("episode_id", "STRING", episode_id),
            bigquery.ScalarQueryParameter("transcribed_time", "TIMESTAMP", time.strftime('%Y-%m-%d %H:%M:%S')),
        ]
    )
    client.query(query_create_temp, job_config=job_config_create_temp).result()

    # Copy the temporary table back to the original table
    query_copy_back = f"""
        INSERT INTO `{table_id}`
        SELECT * FROM `{temp_table_id}`
    """
    client.query(query_copy_back).result()

    # Delete the temporary table
    client.delete_table(temp_table_id, not_found_ok=True)

    logging.info(f"Updated transcript for episode_id: {episode_id}")

def process_transcripts():
    client = bigquery.Client()
    table_id = "flightstudio.youtube_transcript_data.podcast_transcripts"

    query = f"""
        SELECT episode_id, transcript
        FROM `{table_id}`
        WHERE NOT STARTS_WITH(transcript, 'trailer')
    """
    query_job = client.query(query)
    results = query_job.result()

    for row in results:
        episode_id = row.episode_id
        transcript = row.transcript.split('\n')

        # Print the first 15 lines of the transcript
        print(f"Episode ID: {episode_id}")
        for i, line in enumerate(transcript[:25]):
            print(f"{i + 1}: {line}")

        # Prompt for the line number to re-label as "trailer:"
        trailer_end_line = int(input("Enter the line number up to which to label as 'trailer:': "))

        # Update the transcript with "trailer:" label
        for i in range(trailer_end_line):
            if "Steven Bartlett:" in transcript[i]:
                transcript[i] = transcript[i].replace("Steven Bartlett:", "trailer:")
            elif "Interviewee:" in transcript[i]:
                transcript[i] = transcript[i].replace("Interviewee:", "trailer:")

        # Prompt to confirm if the remaining labeling is correct
        print("\n".join(transcript[trailer_end_line:trailer_end_line + 15]))
        correct_labeling = input("Is the remaining labeling correct? (y/n): ").strip().lower()

        if correct_labeling == 'n':
            # Swap "Steven Bartlett" and "Interviewee" labels
            for i in range(trailer_end_line, len(transcript)):
                if "Steven Bartlett:" in transcript[i]:
                    transcript[i] = transcript[i].replace("Steven Bartlett:", "Interviewee:")
                elif "Interviewee:" in transcript[i]:
                    transcript[i] = transcript[i].replace("Interviewee:", "Steven Bartlett:")

        # Join the transcript back into a single string
        new_transcript = "\n".join(transcript)

        # Update the transcript in BigQuery
        update_transcript_labels(client, table_id, episode_id, new_transcript)

if __name__ == "__main__":
    # Set the path to your service account key file
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "flightstudio-d8c6c3039d4c.json"
    process_transcripts()
