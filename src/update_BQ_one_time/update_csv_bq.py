import csv
import logging
import os
from google.cloud import bigquery

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Set the path to your service account key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "flightstudio-d8c6c3039d4c.json"

def update_bq_from_csv():
    client = bigquery.Client()
    table_id = "flightstudio.youtube_transcript_data.podcast_transcripts"
    temp_table_id = "flightstudio.youtube_transcript_data.temp_podcast_transcripts"
    csv_file_path = "podcast_transcripts.csv"

    # Create a temporary table as a copy of the original
    #client.query(f"CREATE TABLE {temp_table_id} AS SELECT * FROM {table_id}").result()

    with open(csv_file_path, mode='r', newline='') as file:
        reader = csv.DictReader(file)
        rows_to_update = [row for row in reader]

    for row in rows_to_update:
        row_id = row['episode_id']
        guest_name = row['guest_name']
        query = f"""
        UPDATE {temp_table_id}
        SET guest_name = @guest_name
        WHERE episode_id = @episode_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("guest_name", "STRING", guest_name),
                bigquery.ScalarQueryParameter("episode_id", "STRING", row_id),
            ]
        )
        client.query(query, job_config=job_config).result()

    logging.info(f"Data updated in temporary table from {csv_file_path}")

if __name__ == "__main__":
    update_bq_from_csv()