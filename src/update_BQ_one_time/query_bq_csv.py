import csv
import logging
import os
from google.cloud import bigquery

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Set the path to your service account key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "flightstudio-d8c6c3039d4c.json"


def export_to_csv():
    client = bigquery.Client()
    query = """
        SELECT episode_id, episode_name, release_date, guest_name, episode_description
        FROM `flightstudio.youtube_transcript_data.podcast_transcripts`
    """
    query_job = client.query(query)
    results = query_job.result()

    # Define the CSV file path
    csv_file_path = "podcast_transcripts.csv"

    # Write results to CSV
    with open(csv_file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        # Write header
        writer.writerow([field.name for field in results.schema])
        # Write data rows
        for row in results:
            writer.writerow([row[field.name] for field in results.schema])

    logging.info(f"Data exported to {csv_file_path}")

if __name__ == "__main__":
    export_to_csv()