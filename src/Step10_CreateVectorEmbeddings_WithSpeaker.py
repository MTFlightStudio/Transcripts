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
script_dir = Path(__file__).parent.resolve()
config_path = script_dir / 'config.env'
load_dotenv(dotenv_path=config_path)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('youtube-transcript-indexer-WITH-SPEAKER')

SERVICE_ACCOUNT_FILE = script_dir / 'flightstudio-d8c6c3039d4c.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(SERVICE_ACCOUNT_FILE)
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not PINECONE_API_KEY or not OPENAI_API_KEY:
    raise ValueError("PINECONE_API_KEY and OPENAI_API_KEY must be set.")

openai_client = OpenAI(api_key=OPENAI_API_KEY)
try:
    pc = Pinecone(api_key=PINECONE_API_KEY)
    logger.info("Pinecone initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize Pinecone: {e}")
    raise

# Define the index for embeddings WITH speaker context
INDEX_NAME = "youtube-transcripts-embeddings-speaker11"
DIMENSION = 1536
METRIC = "cosine"
BATCH_SIZE = 100
EMBEDDING_BATCH_SIZE = 20

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def create_pinecone_index() -> None:
    existing_indexes = pc.list_indexes().names()
    if INDEX_NAME not in existing_indexes:
        logger.info(f"Creating index '{INDEX_NAME}' with dimension={DIMENSION}, metric={METRIC}")
        pc.create_index(
            name=INDEX_NAME,
            dimension=DIMENSION,
            metric=METRIC,
            spec=ServerlessSpec(cloud="gcp", region="europe-west4")
        )
        logger.info("Waiting 30s for index to initialize...")
        time.sleep(30)
    else:
        logger.info(f"Index '{INDEX_NAME}' already exists.")

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def get_existing_vector_ids(index) -> set:
    logger.info("Fetching existing vector IDs from Pinecone...")
    try:
        existing_ids = set()
        for ids_batch in index.list(limit=99):
            existing_ids.update(ids_batch)
        logger.info(f"Found {len(existing_ids)} existing vector IDs.")
        return existing_ids
    except Exception as e:
        logger.error(f"Could not fetch existing IDs from Pinecone: {e}. Assuming empty.", exc_info=True)
        return set()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def get_embeddings_batch(texts: List[str]) -> List[List[float]]:
    response = openai_client.embeddings.create(input=texts, model="text-embedding-ada-002")
    return [item.embedding for item in response.data]

def fetch_data_from_bigquery(existing_ids: set) -> pd.DataFrame:
    logger.info("Initializing BigQuery client...")
    bq_client = bigquery.Client()
    
    if not existing_ids:
        query = "SELECT * FROM `flightstudio.YouTube_RAG_data.processed_chunks_7`"
        logger.info("Querying all chunks as no existing IDs were found...")
    else:
        temp_table_id = f"flightstudio.YouTube_RAG_data.temp_existing_speaker_ids_{int(time.time())}"
        logger.info(f"Uploading {len(existing_ids)} existing chunk IDs to temp table: {temp_table_id}")
        
        df = pd.DataFrame() # Initialize an empty dataframe
        try:
            ids_df = pd.DataFrame(list(existing_ids), columns=["chunk_id"])
            job_config = bigquery.LoadJobConfig(
                schema=[bigquery.SchemaField("chunk_id", "STRING")],
                write_disposition="WRITE_TRUNCATE",
            )
            # Wait for the load job to complete
            bq_client.load_table_from_dataframe(ids_df, temp_table_id, job_config=job_config).result()
            logger.info(f"Temporary table {temp_table_id} created successfully.")
            
            query = f"""
            SELECT t1.*
            FROM `flightstudio.YouTube_RAG_data.processed_chunks_7` AS t1
            LEFT JOIN `{temp_table_id}` AS t2 ON t1.chunk_id = t2.chunk_id
            WHERE t2.chunk_id IS NULL
            """
            logger.info("Querying for new chunks using temporary table...")
            # Execute the query and wait for it to complete
            df = bq_client.query(query).to_dataframe()

        finally:
            # This block will run whether the try block succeeds or fails
            bq_client.delete_table(temp_table_id, not_found_ok=True)
            logger.info(f"Temporary table {temp_table_id} deleted.")

    logger.info(f"Retrieved {len(df)} rows from BigQuery.")
    return df

def prepare_metadata(row: pd.Series) -> Dict[str, Any]:
    metadata = {}
    for col, value in row.items():
        if pd.isna(value):
            continue
        # Simple type casting to avoid Pinecone errors
        if isinstance(value, (np.integer, int)):
            metadata[col] = int(value)
        elif isinstance(value, (np.floating, float)):
            metadata[col] = float(value)
        else:
            metadata[col] = str(value)
        
        # Truncate long fields
        if isinstance(metadata[col], str) and len(metadata[col]) > 500:
             metadata[col] = metadata[col][:500]
    return metadata

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def upsert_vectors_batch(index, vectors: List[Dict]) -> None:
    if not vectors: return
    upsert_data = [(v["id"], v["values"], v["metadata"]) for v in vectors]
    index.upsert(vectors=upsert_data)
    logger.info(f"Successfully upserted batch of {len(vectors)} vectors")

def process_and_upsert_data(df: pd.DataFrame, index) -> None:
    initial_rows = len(df)
    logger.info(f"Starting data processing for {initial_rows} rows from BigQuery.")
    
    # --- 1. PREPARE DATA ---
    df.dropna(subset=['chunk_with_speaker', 'chunk_id'], inplace=True)
    rows_to_process = df.to_dict('records')
    texts_to_embed = [row['chunk_with_speaker'] for row in rows_to_process]
    
    total_to_process = len(rows_to_process)
    skipped = initial_rows - total_to_process
    if skipped > 0:
        logger.warning(f"Skipped {skipped} rows with missing 'chunk_with_speaker' or 'chunk_id'.")
    
    if not texts_to_embed:
        logger.info("No valid data to process after filtering.")
        logger.info(f"Data ingestion complete. Total from BQ: {initial_rows}, Processed: 0, Skipped: {skipped}, Upserted: 0")
        return

    # --- 2. GET EMBEDDINGS IN BATCHES ---
    all_embeddings = []
    logger.info(f"Getting embeddings for {total_to_process} texts in batches of {EMBEDDING_BATCH_SIZE}...")
    for i in range(0, total_to_process, EMBEDDING_BATCH_SIZE):
        batch_texts = texts_to_embed[i:i + EMBEDDING_BATCH_SIZE]
        try:
            embeddings_batch = get_embeddings_batch(batch_texts)
            all_embeddings.extend(embeddings_batch)
            if (i // EMBEDDING_BATCH_SIZE) % 5 == 0: # Log progress every 5 batches
                 logger.info(f"  ... retrieved embeddings for {len(all_embeddings)}/{total_to_process} texts")
        except Exception as e:
            logger.error(f"Failed to get embeddings for batch starting at index {i}. Skipping {len(batch_texts)} items. Error: {e}", exc_info=True)
            all_embeddings.extend([None] * len(batch_texts)) # Add placeholders

    logger.info("Finished getting all embeddings.")

    # --- 3. PREPARE AND UPSERT VECTORS ---
    upserted_count = 0
    processed_count = 0
    
    vectors_to_upsert = []
    final_skipped = skipped
    for row, embedding in zip(rows_to_process, all_embeddings):
        processed_count += 1
        if embedding is None:
            final_skipped += 1
            continue
            
        metadata = prepare_metadata(pd.Series(row))
        vector = {"id": row["chunk_id"], "values": embedding, "metadata": metadata}
        vectors_to_upsert.append(vector)
        
        if len(vectors_to_upsert) >= BATCH_SIZE:
            upsert_vectors_batch(index, vectors_to_upsert)
            upserted_count += len(vectors_to_upsert)
            logger.info(f"Upserted a batch. Total upserted so far: {upserted_count}")
            vectors_to_upsert = []

    if vectors_to_upsert:
        upsert_vectors_batch(index, vectors_to_upsert)
        upserted_count += len(vectors_to_upsert)
        logger.info(f"Upserted final batch. Total upserted: {upserted_count}")
        
    logger.info(f"Data ingestion complete. Total from BQ: {initial_rows}, Processed: {processed_count}, Skipped: {final_skipped}, Upserted: {upserted_count}")

def main():
    start_time = time.time()
    logger.info("--- Starting YouTube Transcript Ingestion (WITH SPEAKER) ---")
    
    try:
        create_pinecone_index()
        index = pc.Index(INDEX_NAME)
        
        existing_ids = get_existing_vector_ids(index)
        df = fetch_data_from_bigquery(existing_ids)
        
        if df.empty:
            logger.info("No new data to process. Exiting.")
            return
        
        process_and_upsert_data(df, index)
        
    except Exception as e:
        logger.error(f"Script failed: {e}", exc_info=True)
    finally:
        elapsed_time = time.time() - start_time
        logger.info(f"Script completed in {elapsed_time:.2f} seconds")

if __name__ == "__main__":
    main() 