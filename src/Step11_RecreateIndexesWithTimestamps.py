import os
import time
import logging
import pandas as pd
import numpy as np
from pinecone import Pinecone, ServerlessSpec
from openai import OpenAI
from google.cloud import bigquery
from typing import List, Dict, Any, Optional
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv
from pathlib import Path

# --- CONFIGURATION ---
# Load environment variables from .env file
script_dir = Path(__file__).parent.resolve()
config_path = script_dir / 'config.env'
load_dotenv(dotenv_path=config_path)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('youtube-transcript-indexer-v2')

# Set your API keys and environment variables
SERVICE_ACCOUNT_FILE = script_dir / 'flightstudio-d8c6c3039d4c.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(SERVICE_ACCOUNT_FILE)
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not PINECONE_API_KEY or not OPENAI_API_KEY:
    raise ValueError("PINECONE_API_KEY and OPENAI_API_KEY must be set in your environment.")

# Initialize OpenAI with new client
openai_client = OpenAI(api_key=OPENAI_API_KEY)

# Initialize Pinecone with the new API
try:
    pc = Pinecone(api_key=PINECONE_API_KEY)
    logger.info("Pinecone initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize Pinecone: {e}")
    raise

# Define the new index names with v2 suffix to indicate new version
INDEX_NAME_NO_SPEAKER = "youtube-transcripts-embeddings-no-speaker-v2"
INDEX_NAME_SPEAKER = "youtube-transcripts-embeddings-speaker-v2"
DIMENSION = 1536  # OpenAI ada-002 embedding dimension
METRIC = "cosine"
BATCH_SIZE = 100  # Number of vectors to upsert at once
EMBEDDING_BATCH_SIZE = 20  # Number of texts to embed at once

# ----------------------------------------------------------------------------------
# CHOOSE WHICH INDEX TO BUILD: SET TO `False` for 'no-speaker', `True` for 'speaker'
# ----------------------------------------------------------------------------------
USE_SPEAKER_INDEX = False
# ----------------------------------------------------------------------------------

INDEX_NAME = INDEX_NAME_SPEAKER if USE_SPEAKER_INDEX else INDEX_NAME_NO_SPEAKER

def convert_date_to_timestamp(date_str: str) -> float:
    """Convert date string to Unix timestamp."""
    if pd.isna(date_str) or not date_str:
        return 0.0
    
    try:
        # Try parsing with time component first
        if ' ' in str(date_str):
            dt = datetime.strptime(str(date_str).split()[0], '%Y-%m-%d')
        else:
            dt = datetime.strptime(str(date_str), '%Y-%m-%d')
        
        # Convert to Unix timestamp (seconds since epoch)
        return dt.timestamp()
    except Exception as e:
        logger.warning(f"Could not parse date '{date_str}': {e}")
        return 0.0

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def create_pinecone_index() -> None:
    """Create Pinecone index if it doesn't exist with retry logic."""
    existing_indexes = pc.list_indexes().names()
    if INDEX_NAME not in existing_indexes:
        logger.info(f"Creating index '{INDEX_NAME}' with dimension={DIMENSION}, metric={METRIC}")
        pc.create_index(
            name=INDEX_NAME,
            dimension=DIMENSION,
            metric=METRIC,
            spec=ServerlessSpec(cloud="gcp", region="europe-west4")
        )
        # Give some time for the index to be ready
        logger.info("Waiting for index to initialize...")
        time.sleep(30)
        logger.info(f"Index '{INDEX_NAME}' created successfully.")
    else:
        logger.info(f"Index '{INDEX_NAME}' already exists.")

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def get_existing_vector_ids(index) -> set:
    """Fetch all existing vector IDs from the Pinecone index to avoid re-processing."""
    logger.info("Fetching existing vector IDs from Pinecone...")
    try:
        existing_ids = set()
        for ids_batch in index.list(limit=99):
            existing_ids.update(ids_batch)
        logger.info(f"Found {len(existing_ids)} existing vector IDs in Pinecone.")
        return existing_ids
    except Exception as e:
        logger.error(f"Could not fetch existing IDs from Pinecone: {e}. Will proceed without checking.")
        return set()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def get_embeddings_batch(texts: List[str]) -> List[List[float]]:
    """Generate embeddings for a batch of texts using OpenAI with new API."""
    try:
        response = openai_client.embeddings.create(
            input=texts,
            model="text-embedding-ada-002"
        )
        return [item.embedding for item in response.data]
    except Exception as e:
        logger.error(f"Error generating embeddings: {e}")
        raise

def fetch_data_from_bigquery(existing_ids: set) -> pd.DataFrame:
    """Fetch transcript data from BigQuery, excluding chunks that are already in Pinecone."""
    logger.info("Initializing BigQuery client...")
    bq_client = bigquery.Client()
    
    # Base query for fetching all columns
    base_query = "SELECT * FROM `flightstudio.YouTube_RAG_data.processed_chunks_7`"

    if not existing_ids:
        logger.info("No existing IDs found in Pinecone. Querying all chunks...")
        try:
            df = bq_client.query(base_query).to_dataframe()
            logger.info(f"Retrieved {len(df)} rows from BigQuery.")
            return df
        except Exception as e:
            logger.error(f"Error querying all chunks from BigQuery: {e}")
            raise

    # If there are existing IDs, use a temporary table to filter them out
    temp_table_id = f"flightstudio.YouTube_RAG_data.temp_existing_ids_{int(time.time())}"
    logger.info(f"Uploading {len(existing_ids)} existing chunk IDs to temporary BigQuery table: {temp_table_id}")
    
    try:
        ids_df = pd.DataFrame(list(existing_ids), columns=["chunk_id"])
        job_config = bigquery.LoadJobConfig(
            schema=[bigquery.SchemaField("chunk_id", "STRING")],
            write_disposition="WRITE_TRUNCATE",
        )
        bq_client.load_table_from_dataframe(ids_df, temp_table_id, job_config=job_config).result()
        logger.info(f"Successfully created temporary table {temp_table_id}.")

        join_query = f"""
        SELECT t1.*
        FROM `flightstudio.YouTube_RAG_data.processed_chunks_7` AS t1
        LEFT JOIN `{temp_table_id}` AS t2 ON t1.chunk_id = t2.chunk_id
        WHERE t2.chunk_id IS NULL
        """
        
        logger.info("Querying BigQuery for new transcript chunks using temporary table...")
        df = bq_client.query(join_query).to_dataframe()
        logger.info(f"Retrieved {len(df)} new rows from BigQuery.")
        return df

    except Exception as e:
        logger.error(f"Error during BigQuery operation with temporary table: {e}")
        raise
    finally:
        logger.info(f"Deleting temporary table {temp_table_id}...")
        bq_client.delete_table(temp_table_id, not_found_ok=True)
        logger.info("Temporary table deleted.")

def prepare_metadata(row: pd.Series) -> Dict[str, Any]:
    """
    Prepare metadata dictionary from DataFrame row with strict type handling.
    IMPORTANT: release_date is now converted to Unix timestamp for proper filtering.
    """
    metadata = {}
    
    # Convert release_date to Unix timestamp
    if "release_date" in row and pd.notna(row["release_date"]):
        release_date_timestamp = convert_date_to_timestamp(row["release_date"])
        if release_date_timestamp > 0:
            metadata["release_date_timestamp"] = release_date_timestamp
    
    # Process each column in the row
    for col, value in row.items():
        if pd.isna(value):
            continue
        
        # --- FIX: Clean up episode_name format ---
        if col == 'episode_name' and isinstance(value, str):
            value = value.replace('_', ' ')

        # Pinecone only supports string, number, boolean, or list of strings for metadata.
        # We will cast everything to a safe type.
        if isinstance(value, (int, np.integer)):
             metadata[col] = float(value) # Store integers as floats
        elif isinstance(value, (float, np.floating)):
             metadata[col] = float(value)
        elif isinstance(value, (bool, np.bool_)):
             metadata[col] = bool(value)
        else:
            # All other types are converted to string
            metadata[col] = str(value)

        # Truncate long text fields to prevent exceeding Pinecone limits
        if isinstance(metadata.get(col), str) and col in ["chunk", "chunk_with_speaker", "episode_name", "episode_description"]:
            metadata[col] = metadata[col][:500]

    return metadata


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def upsert_vectors_batch(index, vectors: List[Dict]) -> None:
    """Upsert a batch of vectors to Pinecone with retry logic."""
    if not vectors:
        return
    
    upsert_data = [
        (
            vector["id"],
            vector["values"],
            vector["metadata"]
        )
        for vector in vectors
    ]
    
    try:
        index.upsert(vectors=upsert_data)
        logger.info(f"Successfully upserted batch of {len(vectors)} vectors")
    except Exception as e:
        logger.error(f"Error upserting vectors: {e}")
        raise

def process_and_upsert_data(df: pd.DataFrame, index) -> None:
    """Process DataFrame rows and upsert vectors to Pinecone."""
    total_rows = len(df)
    processed = 0
    skipped = 0
    upserted = 0
    
    batch_texts = []
    batch_rows = []
    vectors_to_upsert = []
    
    for idx, row in df.iterrows():
        # Choose text based on index type
        vector_text = row.get("chunk_with_speaker") if USE_SPEAKER_INDEX else row.get("chunk")
        
        if not vector_text or pd.isna(vector_text):
            logger.warning(f"Skipping row {idx}: Missing or invalid text")
            skipped += 1
            continue
        
        batch_texts.append(vector_text)
        batch_rows.append(row)
        
        if len(batch_texts) >= EMBEDDING_BATCH_SIZE:
            try:
                embeddings = get_embeddings_batch(batch_texts)
                
                for i, embedding in enumerate(embeddings):
                    current_row = batch_rows[i]
                    metadata = prepare_metadata(current_row)
                    vector = {
                        "id": current_row["chunk_id"],
                        "values": embedding,
                        "metadata": metadata
                    }
                    vectors_to_upsert.append(vector)
                
                if len(vectors_to_upsert) >= BATCH_SIZE:
                    upsert_vectors_batch(index, vectors_to_upsert[:BATCH_SIZE])
                    upserted += len(vectors_to_upsert[:BATCH_SIZE])
                    vectors_to_upsert = vectors_to_upsert[BATCH_SIZE:]
                
                batch_texts = []
                batch_rows = []
                
            except Exception as e:
                logger.error(f"Error processing batch: {e}")
                skipped += len(batch_texts)
                batch_texts = []
                batch_rows = []
        
        processed += 1
        if processed % 500 == 0:
            logger.info(f"Progress: {processed}/{total_rows} rows processed ({(processed/total_rows)*100:.1f}%)")
    
    # Process any remaining items in the last batch
    if batch_texts:
        try:
            embeddings = get_embeddings_batch(batch_texts)
            for i, embedding in enumerate(embeddings):
                current_row = batch_rows[i]
                metadata = prepare_metadata(current_row)
                vector = {
                    "id": current_row["chunk_id"],
                    "values": embedding,
                    "metadata": metadata
                }
                vectors_to_upsert.append(vector)
        except Exception as e:
            logger.error(f"Error processing final batch: {e}")
            skipped += len(batch_texts)
    
    # Upsert any remaining vectors
    while vectors_to_upsert:
        upsert_vectors_batch(index, vectors_to_upsert[:BATCH_SIZE])
        upserted += len(vectors_to_upsert[:BATCH_SIZE])
        vectors_to_upsert = vectors_to_upsert[BATCH_SIZE:]
    
    logger.info(f"Data ingestion complete.")
    logger.info(f"Total rows: {total_rows}")
    logger.info(f"Processed: {processed}")
    logger.info(f"Skipped: {skipped}")
    logger.info(f"Upserted: {upserted}")


def main():
    """Main function to run the script."""
    start_time = time.time()
    index_type = "speaker" if USE_SPEAKER_INDEX else "no-speaker"
    logger.info(f"--- Starting YouTube Transcript Ingestion (v2 - {index_type}) ---")
    
    try:
        create_pinecone_index()
        index = pc.Index(INDEX_NAME)
        
        # Initialize the BigQuery client
        bq_client = bigquery.Client()
        
        # Since we are creating new v2 indexes, we should process all data, not just new data.
        # We will fetch all data from BigQuery and not check for existing IDs.
        logger.info("Fetching all transcript data from BigQuery to build new v2 index...")
        df = bq_client.query("SELECT * FROM `flightstudio.YouTube_RAG_data.processed_chunks_7`").to_dataframe()
        
        if df.empty:
            logger.info("No data found in BigQuery. Exiting.")
            return
        
        process_and_upsert_data(df, index)
        
        elapsed_time = time.time() - start_time
        logger.info(f"Script completed successfully in {elapsed_time:.2f} seconds")
    
    except Exception as e:
        logger.error(f"Script failed: {e}", exc_info=True)
        elapsed_time = time.time() - start_time
        logger.error(f"Script failed after {elapsed_time:.2f} seconds")
        raise

if __name__ == "__main__":
    main() 