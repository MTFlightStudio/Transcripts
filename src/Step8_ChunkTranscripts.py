import logging
import os
import time
import pandas as pd
import nltk
from nltk.tokenize import sent_tokenize
import uuid
from google.cloud import bigquery

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Set the path to your service account key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "flightstudio-d8c6c3039d4c.json"

# Download NLTK data if not already available
nltk.download('punkt', quiet=True)

PROCESSED_CHUNKS_TABLE = 'flightstudio.YouTube_RAG_data.processed_chunks_7'
SOURCE_TABLE = 'flightstudio.YouTube_RAG_data.transcripts_split_with_intensity_and_retention'

def create_chunk(current_chunk, min_chunk_size, max_chunk_size, starting_chunk_index):
    chunks = []
    temp_chunk = []
    temp_labeled_chunk = []
    temp_word_count = 0
    temp_rows = []  # Accumulate corresponding rows
    chunk_index = starting_chunk_index  # Start from the given chunk index

    idx = 0
    while idx < len(current_chunk):
        r = current_chunk[idx]
        text = r['line_text']
        labeled_text = f"{r['speaker']}: {r['line_text']}"
        word_count = len(text.split())

        # Check if the individual row exceeds max_chunk_size
        if word_count > max_chunk_size:
            # Split the row into sentences
            sentences = nltk.sent_tokenize(text)
            sentence_idx = 0
            while sentence_idx < len(sentences):
                sentence = sentences[sentence_idx]
                sentence_word_count = len(sentence.split())

                # If adding the sentence exceeds max_chunk_size, create a chunk
                if temp_word_count + sentence_word_count > max_chunk_size and temp_word_count >= min_chunk_size:
                    # Create chunk with current temp_chunk
                    chunk_text = " ".join(temp_chunk)
                    chunk_labeled_text = " ".join(temp_labeled_chunk)
                    chunked_data = create_chunk_data(temp_rows, chunk_text, chunk_labeled_text)
                    chunked_data['chunk_index'] = chunk_index
                    chunks.append(chunked_data)
                    chunk_index += 1
                    # Reset temp variables
                    temp_chunk = []
                    temp_labeled_chunk = []
                    temp_word_count = 0
                    temp_rows = []
                else:
                    temp_chunk.append(sentence)
                    temp_labeled_chunk.append(f"{r['speaker']}: {sentence}")
                    temp_word_count += sentence_word_count
                    sentence_idx += 1
                    if not temp_rows:
                        temp_rows.append(r)  # Add the row once
            # Handle any remaining sentences
            if temp_chunk:
                chunk_text = " ".join(temp_chunk)
                chunk_labeled_text = " ".join(temp_labeled_chunk)
                chunked_data = create_chunk_data(temp_rows, chunk_text, chunk_labeled_text)
                chunked_data['chunk_index'] = chunk_index
                chunks.append(chunked_data)
                chunk_index += 1
                # Reset temp variables
                temp_chunk = []
                temp_labeled_chunk = []
                temp_word_count = 0
                temp_rows = []
            idx += 1
        else:
            # Normal processing for rows within max_chunk_size
            if temp_word_count + word_count > max_chunk_size and temp_word_count >= min_chunk_size:
                # Create chunk with current temp_chunk
                chunk_text = " ".join(temp_chunk)
                chunk_labeled_text = " ".join(temp_labeled_chunk)
                chunked_data = create_chunk_data(temp_rows, chunk_text, chunk_labeled_text)
                chunked_data['chunk_index'] = chunk_index
                chunks.append(chunked_data)
                chunk_index += 1
                # Reset temp variables
                temp_chunk = []
                temp_labeled_chunk = []
                temp_word_count = 0
                temp_rows = []

            temp_chunk.append(text)
            temp_labeled_chunk.append(labeled_text)
            temp_word_count += word_count
            temp_rows.append(r)
            idx += 1

    # Add any remaining lines
    if temp_chunk:
        chunk_text = " ".join(temp_chunk)
        chunk_labeled_text = " ".join(temp_labeled_chunk)
        chunked_data = create_chunk_data(temp_rows, chunk_text, chunk_labeled_text)
        chunked_data['chunk_index'] = chunk_index
        chunks.append(chunked_data)
        chunk_index += 1

    return chunks, chunk_index  # Return the updated chunk index

def create_chunk_data(current_chunk, chunk_text, chunk_labeled_text):
    chunked_data = {}
    chunked_data['chunk'] = chunk_text  # Text without speaker labels
    chunked_data['chunk_with_speaker'] = chunk_labeled_text  # Text with speaker labels
    chunked_data['chunk_id'] = str(uuid.uuid4())
    # 'chunk_index' will be assigned in the calling function
    
    # Handle metadata
    first_row = current_chunk[0]
    last_row = current_chunk[-1]
    chunked_data['episode_id'] = first_row['episode_id']
    chunked_data['episode_name'] = first_row['episode_name']
    chunked_data['release_date'] = first_row['release_date']
    chunked_data['guest_name'] = first_row['guest_name']
    chunked_data['episode_description'] = first_row['episode_description']
    
    # Collect unique speakers
    speakers = set(r['speaker'] for r in current_chunk)
    chunked_data['speaker'] = " / ".join(speakers)
    
    # Start and end times
    chunked_data['start_time_seconds'] = first_row['start_time_seconds']
    chunked_data['end_time_seconds'] = last_row['end_time_seconds']
    chunked_data['chunk_start_time'] = first_row['start_time_seconds']
    chunked_data['chunk_end_time'] = last_row['end_time_seconds']
    
    # Original times
    chunked_data['original_start_time_seconds'] = first_row['start_time_seconds']
    chunked_data['original_end_time_seconds'] = last_row['end_time_seconds']
    
    # Handle numerical metadata
    numerical_fields = ['views', 'estimatedMinutesWatched', 'averageViewDuration',
                        'averageViewPercentage', 'subscribersGained', 'subscribersLost',
                        'likes', 'dislikes', 'comments', 'shares', 'estimatedRevenue',
                        'intensityScoreNormalized', 'relativeRetentionPerformance',
                        'audienceWatchRatio']
    for field in numerical_fields:
        values = [r[field] for r in current_chunk if field in r and r[field] is not None]
        chunked_data[field] = values[0] if values else None  # Or handle as needed
    
    return chunked_data

def update_table_schema(client, table_id):
    table = client.get_table(table_id)
    
    new_schema = table.schema[:]  # Create a copy of the existing schema
    
    # Check and add new fields if they don't exist
    field_names = [field.name for field in table.schema]
    
    new_fields = [
        ("chunk_index", "INTEGER"),
        ("chunk_with_speaker", "STRING"),  # New field
        ("chunk_start_time", "FLOAT"),
        ("chunk_end_time", "FLOAT"),
        ("original_start_time_seconds", "FLOAT"),
        ("original_end_time_seconds", "FLOAT")
    ]
    
    for field_name, field_type in new_fields:
        if field_name not in field_names:
            new_schema.append(bigquery.SchemaField(field_name, field_type))
    
    if new_schema != table.schema:
        table.schema = new_schema
        client.update_table(table, ["schema"])
        logging.info(f"Updated schema for table {table_id}")
    else:
        logging.info(f"Schema for table {table_id} is up to date")

def save_chunked_data_to_bq(client, chunked_df, destination_table):
    schema = [
        bigquery.SchemaField("chunk_id", "STRING"),
        bigquery.SchemaField("episode_id", "STRING"),
        bigquery.SchemaField("episode_name", "STRING"),
        bigquery.SchemaField("release_date", "DATE"),
        bigquery.SchemaField("guest_name", "STRING"),
        bigquery.SchemaField("episode_description", "STRING"),
        bigquery.SchemaField("speaker", "STRING"),
        bigquery.SchemaField("chunk", "STRING"),
        bigquery.SchemaField("chunk_with_speaker", "STRING"),  # New column
        bigquery.SchemaField("chunk_index", "INTEGER"),
        bigquery.SchemaField("chunk_start_time", "FLOAT"),
        bigquery.SchemaField("chunk_end_time", "FLOAT"),
        bigquery.SchemaField("original_start_time_seconds", "FLOAT"),
        bigquery.SchemaField("original_end_time_seconds", "FLOAT"),
        bigquery.SchemaField("views", "INTEGER"),
        bigquery.SchemaField("estimatedMinutesWatched", "INTEGER"),
        bigquery.SchemaField("averageViewDuration", "INTEGER"),
        bigquery.SchemaField("averageViewPercentage", "FLOAT"),
        bigquery.SchemaField("subscribersGained", "INTEGER"),
        bigquery.SchemaField("subscribersLost", "INTEGER"),
        bigquery.SchemaField("likes", "INTEGER"),
        bigquery.SchemaField("dislikes", "INTEGER"),
        bigquery.SchemaField("comments", "INTEGER"),
        bigquery.SchemaField("shares", "INTEGER"),
        bigquery.SchemaField("estimatedRevenue", "FLOAT"),
        bigquery.SchemaField("intensityScoreNormalized", "FLOAT"),
        bigquery.SchemaField("relativeRetentionPerformance", "FLOAT"),
        bigquery.SchemaField("audienceWatchRatio", "FLOAT")
    ]

    required_columns = [field.name for field in schema]
    for column in required_columns:
        if column not in chunked_df.columns:
            chunked_df[column] = None

    chunked_df = chunked_df[required_columns].copy()
    chunked_df['release_date'] = pd.to_datetime(chunked_df['release_date']).dt.date

    job_config = bigquery.LoadJobConfig(schema=schema)
    job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

    job = client.load_table_from_dataframe(chunked_df, destination_table, job_config=job_config)
    job.result()
    logging.info(f"Inserted {len(chunked_df)} rows into {destination_table}.")

def get_total_episode_count(client):
    query = f"""
        SELECT COUNT(DISTINCT episode_id) as total_episodes
        FROM `{SOURCE_TABLE}`
    """
    query_job = client.query(query)
    result = query_job.result()
    return result.to_dataframe()['total_episodes'][0]

def get_processed_episodes(client):
    query = f"""
    SELECT DISTINCT episode_id
    FROM `{PROCESSED_CHUNKS_TABLE}`
    """
    query_job = client.query(query)
    return set(row['episode_id'] for row in query_job.result())

def check_unprocessed_episodes(client):
    query = f"""
    SELECT COUNT(DISTINCT t.episode_id) as unprocessed_count
    FROM `{SOURCE_TABLE}` t
    LEFT JOIN `{PROCESSED_CHUNKS_TABLE}` p
    ON t.episode_id = p.episode_id
    WHERE p.episode_id IS NULL
    """
    result = client.query(query).result()
    return result.to_dataframe()['unprocessed_count'][0]

def combine_and_chunk_rows(rows, min_chunk_size=100, max_chunk_size=200):
    combined_chunks = []
    chunk_index_counter = 0  # Initialize chunk index counter
    current_chunk = []
    current_word_count = 0

    for idx, row in enumerate(rows):
        row_word_count = len(row['line_text'].split())
        row['row_word_count'] = row_word_count  # Add word count to row for later use

        current_chunk.append(row)
        current_word_count += row_word_count

        is_last_row = idx == len(rows) - 1

        # Check if we have reached the minimum chunk size or it's the last row
        if current_word_count >= min_chunk_size or is_last_row:
            # Create chunks from the accumulated rows
            chunks_from_current, updated_chunk_index = create_chunk(current_chunk, min_chunk_size, max_chunk_size, chunk_index_counter)
            combined_chunks.extend(chunks_from_current)
            # Update chunk_index_counter
            chunk_index_counter = updated_chunk_index
            # Reset current chunk and counters
            current_chunk = []
            current_word_count = 0

    return combined_chunks

def process_data_in_batches():
    client = bigquery.Client()
    
    # Update the schema of the processed chunks table
    update_table_schema(client, PROCESSED_CHUNKS_TABLE)
    
    total_rows_processed = 0
    processed_episodes = get_processed_episodes(client)
    total_episodes_processed = len(processed_episodes)
    
    logging.info("Starting data processing.")
    total_episodes = get_total_episode_count(client)
    logging.info(f"Total number of episodes to process: {total_episodes}")
    logging.info(f"Already processed episodes: {total_episodes_processed}")
    
    # Fetch all unprocessed episode IDs
    if processed_episodes:
        query = f"""
            SELECT DISTINCT episode_id, release_date
            FROM `{SOURCE_TABLE}`
            WHERE episode_id NOT IN UNNEST({list(processed_episodes)})
            ORDER BY release_date DESC, episode_id ASC
        """

    else:
        query = f"""
            SELECT DISTINCT episode_id, release_date
            FROM `{SOURCE_TABLE}`
            ORDER BY release_date DESC, episode_id ASC
        """
    unprocessed_episodes_job = client.query(query)
    unprocessed_episodes = [row['episode_id'] for row in unprocessed_episodes_job.result()]
    
    for episode_id in unprocessed_episodes:
        try:
            # Fetch all rows for the current episode
            query = f"""
                SELECT episode_id, episode_name, release_date, guest_name, episode_description, speaker, line_text, 
                       start_time_seconds, end_time_seconds, views, estimatedMinutesWatched, averageViewDuration, 
                       averageViewPercentage, subscribersGained, subscribersLost, likes, dislikes, comments, shares, 
                       estimatedRevenue, intensityScoreNormalized, relativeRetentionPerformance, audienceWatchRatio
                FROM `{SOURCE_TABLE}`
                WHERE episode_id = '{episode_id}'
                ORDER BY start_time_seconds ASC
            """
            episode_job = client.query(query)
            episode_df = episode_job.result().to_dataframe()
            
            if episode_df.empty:
                logging.info(f"No data found for episode {episode_id}. Skipping.")
                continue
            
            logging.info(f"Processing episode {episode_id} with {len(episode_df)} rows.")
            
            # Process the episode
            episode_df = episode_df.sort_values('start_time_seconds')
            rows = episode_df.to_dict('records')
            combined_chunks = combine_and_chunk_rows(rows, min_chunk_size=100, max_chunk_size=200)
            
            chunked_df = pd.DataFrame(combined_chunks)
            save_chunked_data_to_bq(client, chunked_df, PROCESSED_CHUNKS_TABLE)
            
            # Mark the episode as processed
            processed_episodes.add(episode_id)
            total_episodes_processed += 1
            total_rows_processed += len(episode_df)
            
            logging.info(f"Processed {total_episodes_processed} episodes out of {total_episodes} total episodes.")
        
        except Exception as e:
            logging.error(f"Error processing episode {episode_id}: {e}")
            time.sleep(10)  # Wait before retrying
            continue
    
    logging.info(f"All rows processed. Total rows processed: {total_rows_processed}.")
    logging.info("Data processing completed.")
    logging.info(f"Total episodes processed: {total_episodes_processed}")
    
    unprocessed_count = check_unprocessed_episodes(client)
    if unprocessed_count > 0:
        logging.warning(f"There are still {unprocessed_count} unprocessed episodes.")
    else:
        logging.info("All episodes have been processed successfully.")

if __name__ == "__main__":
    process_data_in_batches()

