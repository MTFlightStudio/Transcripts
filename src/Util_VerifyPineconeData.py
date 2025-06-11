import os
import logging
from pinecone import Pinecone
from google.cloud import bigquery
from dotenv import load_dotenv
from pathlib import Path
import pandas as pd
from typing import Optional

# --- CONFIGURATION ---
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables from config.env
try:
    script_dir = Path(__file__).parent.resolve()
    config_path = script_dir / 'config.env'
    load_dotenv(dotenv_path=config_path)
    logger.info("Loaded environment variables from config.env")
except Exception as e:
    logger.error(f"Could not load config.env file: {e}")
    # Still try to fall back to system environment variables

# Pinecone and BigQuery details from environment
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
INDEX_NAME = "youtube-transcripts-embeddings-no-speaker11"  # Your production index
PROJECT_ID = "flightstudio"
DATASET_ID = "YouTube_RAG_data"
TABLE_ID = "processed_chunks_7"
SERVICE_ACCOUNT_FILE = script_dir / 'flightstudio-d8c6c3039d4c.json'

def get_latest_episode_from_bq() -> Optional[pd.Series]:
    """Queries BigQuery to get the ID and name of the most recent episode."""
    logger.info("Connecting to BigQuery to find the latest episode...")
    try:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(SERVICE_ACCOUNT_FILE)
        bq_client = bigquery.Client(project=PROJECT_ID)
        
        query = f"""
            SELECT episode_id, episode_name, release_date
            FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
            ORDER BY release_date DESC
            LIMIT 1
        """
        
        df = bq_client.query(query).to_dataframe()
        
        if not df.empty:
            latest_episode = df.iloc[0]
            logger.info(f"Latest episode found: '{latest_episode['episode_name']}' (ID: {latest_episode['episode_id']})")
            return latest_episode
        else:
            logger.warning("Could not find any episodes in the BigQuery table.")
            return None
            
    except Exception as e:
        logger.error(f"Failed to query BigQuery: {e}")
        return None

def verify_episode_in_pinecone(episode_id: str):
    """Queries Pinecone to verify if data for a given episode_id exists."""
    if not PINECONE_API_KEY:
        logger.error("PINECONE_API_KEY is not set. Cannot connect to Pinecone.")
        return

    logger.info(f"Connecting to Pinecone and querying for episode_id: {episode_id}")
    try:
        pc = Pinecone(api_key=PINECONE_API_KEY)
        index = pc.Index(INDEX_NAME)

        # We query with a dummy vector because we are only interested in the metadata filter.
        # This is a common pattern for metadata-based lookups.
        dummy_vector = [0.0] * 1536  # Dimension for text-embedding-ada-002
        
        query_response = index.query(
            vector=dummy_vector,
            filter={"episode_id": {"$eq": episode_id}},
            top_k=5,  # Fetch the top 5 chunks for this episode
            include_metadata=True
        )

        if query_response['matches']:
            logger.info(f"SUCCESS: Found {len(query_response['matches'])} matching vectors for episode {episode_id} in Pinecone.")
            print("\n--- Sample Chunks from the Latest Episode ---")
            for i, match in enumerate(query_response['matches']):
                metadata = match.get('metadata', {})
                chunk_text = metadata.get('chunk', 'N/A')
                chunk_index = metadata.get('chunk_index', 'N/A')
                score = match['score']
                print(f"\nChunk #{i+1} (Index: {chunk_index}, Score: {score:.4f}):")
                print(f"'{chunk_text[:300]}...'") # Print first 300 chars
            print("\n--- End of Sample ---")
        else:
            logger.warning(f"VERIFICATION FAILED: No vectors found for episode_id '{episode_id}' in Pinecone.")

    except Exception as e:
        logger.error(f"An error occurred while querying Pinecone: {e}", exc_info=True)


def main():
    """Main function to run the verification."""
    logger.info("--- Starting Pinecone Data Verification ---")
    latest_episode = get_latest_episode_from_bq()
    
    if latest_episode is not None and 'episode_id' in latest_episode:
        verify_episode_in_pinecone(latest_episode['episode_id'])
    else:
        logger.error("Could not retrieve latest episode details. Aborting verification.")
        
    logger.info("--- Verification Complete ---")

if __name__ == "__main__":
    main() 