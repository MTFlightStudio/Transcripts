import os
import time
import logging
import pandas as pd
import numpy as np
from pinecone import Pinecone, ServerlessSpec
from openai import OpenAI  # Using the new OpenAI client
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
logger = logging.getLogger('youtube-transcript-indexer-no-speaker')

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

# Define the new index name and configuration
INDEX_NAME = "youtube-transcripts-embeddings-no-speaker11"  # New index name without speaker
DIMENSION = 1536  # OpenAI ada-002 embedding dimension
METRIC = "cosine"
BATCH_SIZE = 100  # Number of vectors to upsert at once
EMBEDDING_BATCH_SIZE = 20  # Number of texts to embed at once

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
    """
    Fetch all existing vector IDs from the Pinecone index to avoid re-processing.
    It appears the index.list() method is a generator that yields batches of IDs.
    This function correctly iterates through the generator to build a complete set of IDs.
    """
    logger.info("Fetching existing vector IDs from Pinecone...")
    try:
        existing_ids = set()
        # The .list() method on a Pinecone index returns a generator that yields batches of IDs.
        # The API is currently enforcing a limit of < 100, so we use 99.
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
        # Extract embeddings from the new response format
        return [item.embedding for item in response.data]
    except Exception as e:
        logger.error(f"Error generating embeddings: {e}")
        raise

def fetch_data_from_bigquery(existing_ids: set) -> pd.DataFrame:
    """
    Fetch transcript data from BigQuery, excluding chunks that are already in Pinecone.
    To handle a large number of existing IDs, this function uploads them to a temporary
    BigQuery table and uses a LEFT JOIN to find new chunks, avoiding query length limits.
    """
    logger.info("Initializing BigQuery client...")
    bq_client = bigquery.Client()
    
    # Base query for fetching all columns
    base_query = """
    SELECT 
      chunk_id, episode_id, episode_name, release_date, guest_name, episode_description, 
      speaker, chunk, chunk_index, chunk_start_time, chunk_end_time, 
      original_start_time_seconds as start_time, original_end_time_seconds as end_time, 
      views, estimatedMinutesWatched, averageViewDuration, averageViewPercentage, 
      subscribersGained, subscribersLost, likes, dislikes, comments, shares, 
      estimatedRevenue, intensityScoreNormalized, relativeRetentionPerformance, 
      audienceWatchRatio, chunk_with_speaker
    FROM `flightstudio.YouTube_RAG_data.processed_chunks_7`
    """

    # If no existing IDs, just fetch everything.
    if not existing_ids:
        logger.info("No existing IDs found in Pinecone. Querying all chunks...")
        try:
            df = bq_client.query(base_query).to_dataframe()
            logger.info(f"Retrieved {len(df)} rows from BigQuery.")
            return df
        except Exception as e:
            logger.error(f"Error querying all chunks from BigQuery: {e}")
            raise

    # If there are existing IDs, use a temporary table to filter them out.
    temp_table_id = f"flightstudio.YouTube_RAG_data.temp_existing_ids_{int(time.time())}"
    logger.info(f"Uploading {len(existing_ids)} existing chunk IDs to temporary BigQuery table: {temp_table_id}")
    
    try:
        # Create a DataFrame and load it into the temp table
        ids_df = pd.DataFrame(list(existing_ids), columns=["chunk_id"])
        job_config = bigquery.LoadJobConfig(
            schema=[bigquery.SchemaField("chunk_id", "STRING")],
            write_disposition="WRITE_TRUNCATE",
        )
        bq_client.load_table_from_dataframe(ids_df, temp_table_id, job_config=job_config).result()
        logger.info(f"Successfully created temporary table {temp_table_id}.")

        # Use a LEFT JOIN to find chunks in the main table that are NOT in the temp table
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
        # Clean up by deleting the temporary table
        logger.info(f"Deleting temporary table {temp_table_id}...")
        bq_client.delete_table(temp_table_id, not_found_ok=True)
        logger.info("Temporary table deleted.")

def prepare_metadata(row: pd.Series) -> Dict[str, Any]:
    """
    Prepare metadata dictionary from DataFrame row with strict type handling
    based on the field type expectations from Pinecone.
    """
    # Define fields that should be numeric (float) - marked with "123" in UI
    numeric_fields = [
        "audienceWatchRatio", 
        "averageViewDuration", 
        "averageViewPercentage",
        "chunk_end_time",
        "chunk_index",
        "chunk_start_time",
        "comments",
        "dislikes",
        "end_time",
        "estimatedMinutesWatched",
        "estimatedRevenue",
        "intensityScoreNormalized",
        "likes",
        "relativeRetentionPerformance",
        "shares",
        "start_time",
        "subscribersGained",
        "subscribersLost",
        "views"
    ]
    
    # Define fields that should be text (string) - marked with "Aa" in UI
    text_fields = [
        "chunk",
        "chunk_with_speaker",
        "episode_id",
        "episode_name",
        "guest_name",
        "release_date",
        "speaker",
        "episode_description"
    ]
    
    metadata = {}
    
    # Process each column in the row
    for col in row.index:
        value = row[col]
        
        # Skip null values entirely - don't include them in metadata
        if pd.isna(value):
            continue
        
        # Handle numeric fields
        if col in numeric_fields:
            # Ensure value is converted to float
            try:
                metadata[col] = float(value)
            except (ValueError, TypeError):
                # If conversion fails, skip this field
                logger.warning(f"Could not convert {col}={value} to float, skipping")
                continue
        
        # Handle text fields
        elif col in text_fields:
            # Ensure value is converted to string
            metadata[col] = str(value)
            
            # Truncate long text fields to prevent exceeding Pinecone limits
            if col in ["chunk", "chunk_with_speaker", "episode_name", "episode_description"]:
                metadata[col] = metadata[col][:500]
        
        # For any other fields not explicitly defined, make best guess
        else:
            # Try to determine if it should be numeric
            if isinstance(value, (int, float)):
                metadata[col] = float(value)
            else:
                metadata[col] = str(value)
    
    return metadata

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def upsert_vectors_batch(index, vectors: List[Dict]) -> None:
    """Upsert a batch of vectors to Pinecone with retry logic."""
    if not vectors:
        return
    
    # Prepare data in the format expected by the new Pinecone API
    upsert_data = [
        (
            vector["id"],  # Vector ID
            vector["values"],  # Vector values/embeddings
            vector["metadata"]  # Metadata
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
    # Track statistics
    total_rows = len(df)
    processed = 0
    skipped = 0
    upserted = 0
    
    # Process in batches for both embedding and upserting
    batch_texts = []
    batch_rows = []
    vectors_to_upsert = []
    
    for idx, row in df.iterrows():
        # Get the text for embedding - use chunk instead of chunk_with_speaker
        vector_text = row.get("chunk")
        
        # Skip if text is missing
        if not vector_text or pd.isna(vector_text):
            logger.warning(f"Skipping row {idx}: Missing or invalid text")
            skipped += 1
            continue
        
        # Add to current batch
        batch_texts.append(vector_text)
        batch_rows.append(row)
        
        # Process batch if it reaches embedding batch size
        if len(batch_texts) >= EMBEDDING_BATCH_SIZE:
            # Get embeddings for the batch
            try:
                embeddings = get_embeddings_batch(batch_texts)
                
                # Prepare vectors for each embedding
                for i, embedding in enumerate(embeddings):
                    row = batch_rows[i]
                    metadata = prepare_metadata(row)
                    vector = {
                        "id": row["chunk_id"],
                        "values": embedding,
                        "metadata": metadata
                    }
                    vectors_to_upsert.append(vector)
                    upserted += 1
                
                # Upsert if we've reached the upsert batch size
                if len(vectors_to_upsert) >= BATCH_SIZE:
                    upsert_vectors_batch(index, vectors_to_upsert)
                    vectors_to_upsert = []
                
                # Clear batches
                batch_texts = []
                batch_rows = []
                
            except Exception as e:
                logger.error(f"Error processing batch: {e}")
                # Skip the entire batch if there's an error
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
                row = batch_rows[i]
                metadata = prepare_metadata(row)
                vector = {
                    "id": row["chunk_id"],
                    "values": embedding,
                    "metadata": metadata
                }
                vectors_to_upsert.append(vector)
                upserted += 1
        except Exception as e:
            logger.error(f"Error processing final batch: {e}")
            skipped += len(batch_texts)
    
    # Upsert any remaining vectors
    if vectors_to_upsert:
        upsert_vectors_batch(index, vectors_to_upsert)
    
    # Log final statistics
    logger.info(f"Data ingestion complete.")
    logger.info(f"Total rows: {total_rows}")
    logger.info(f"Processed: {processed}")
    logger.info(f"Skipped: {skipped}")
    logger.info(f"Upserted: {upserted}")

def main():
    """Main function to run the script."""
    start_time = time.time()
    logger.info("Starting YouTube transcript ingestion process (no speaker version)")
    
    try:
        # Create or connect to Pinecone index
        create_pinecone_index()
        index = pc.Index(INDEX_NAME)
        
        # Get IDs that already exist in Pinecone to avoid re-processing
        existing_ids = get_existing_vector_ids(index)
        
        # Fetch only new data from BigQuery
        df = fetch_data_from_bigquery(existing_ids)
        
        if df.empty:
            logger.info("No new data to process. Exiting.")
            return
        
        # Process and upsert data
        process_and_upsert_data(df, index)
        
        elapsed_time = time.time() - start_time
        logger.info(f"Script completed successfully in {elapsed_time:.2f} seconds")
    
    except Exception as e:
        logger.error(f"Script failed: {e}")
        elapsed_time = time.time() - start_time
        logger.error(f"Script failed after {elapsed_time:.2f} seconds")
        raise

if __name__ == "__main__":
    main()