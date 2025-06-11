import os
import logging
from pinecone import Pinecone
from google.cloud import bigquery
from dotenv import load_dotenv
from pathlib import Path
from tqdm import tqdm

# --- CONFIGURATION ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

script_dir = Path(__file__).parent.resolve()
config_path = script_dir / 'config.env'
load_dotenv(dotenv_path=config_path)

PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
INDEX_NAME = "youtube-transcripts-embeddings-speaker11"
PROJECT_ID = "flightstudio"
DATASET_ID = "YouTube_RAG_data"
TABLE_ID = "processed_chunks_7"
SERVICE_ACCOUNT_FILE = script_dir / 'flightstudio-d8c6c3039d4c.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(SERVICE_ACCOUNT_FILE)

# --- BATCH SIZES ---
# Pinecone API limits: list()=1000/req, fetch()=100/req
LIST_BATCH_SIZE = 1000 
FETCH_BATCH_SIZE = 100

def get_bq_episode_ids() -> set:
    """Gets all unique episode_ids from the BigQuery table."""
    logger.info("Connecting to BigQuery to fetch all unique episode IDs...")
    try:
        client = bigquery.Client(project=PROJECT_ID)
        query = f"SELECT DISTINCT episode_id FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`"
        df = client.query(query).to_dataframe()
        ids = set(df['episode_id'].dropna())
        logger.info(f"Found {len(ids)} unique episode IDs in BigQuery.")
        return ids
    except Exception as e:
        logger.error(f"Failed to fetch episode IDs from BigQuery: {e}")
        return set()

def get_pinecone_episode_ids() -> set:
    """
    Gets all unique episode_ids from the Pinecone index.
    This is done by listing all chunk_ids and then fetching their metadata in batches.
    """
    if not PINECONE_API_KEY:
        logger.error("PINECONE_API_KEY is not set. Cannot connect to Pinecone.")
        return set()

    logger.info(f"Connecting to Pinecone index '{INDEX_NAME}'...")
    try:
        pc = Pinecone(api_key=PINECONE_API_KEY)
        index = pc.Index(INDEX_NAME)

        logger.info("Step 1/3: Listing all vector IDs from Pinecone...")
        # The list method is a generator that paginates automatically.
        # We must iterate through it to flatten the list of lists it returns.
        all_chunk_ids = []
        for ids_batch in index.list():
            all_chunk_ids.extend(ids_batch)
        total_vectors = len(all_chunk_ids)
        logger.info(f"Found {total_vectors} total vectors.")

        pinecone_episode_ids = set()
        logger.info("Step 2/3: Fetching metadata for all vectors in batches...")
        
        with tqdm(total=total_vectors, desc="Fetching Pinecone Metadata") as pbar:
            for i in range(0, total_vectors, FETCH_BATCH_SIZE):
                batch_ids = all_chunk_ids[i:i + FETCH_BATCH_SIZE]
                if not batch_ids:
                    continue
                
                try:
                    fetch_response = index.fetch(ids=batch_ids)
                    vectors = fetch_response.get('vectors', {})
                    for vec_id, vector_data in vectors.items():
                        metadata = vector_data.get('metadata', {})
                        if 'episode_id' in metadata:
                            pinecone_episode_ids.add(metadata['episode_id'])
                except Exception as e:
                    logger.warning(f"Failed to fetch batch starting at index {i}: {e}")
                
                pbar.update(len(batch_ids))

        logger.info("Step 3/3: Metadata fetch complete.")
        logger.info(f"Found {len(pinecone_episode_ids)} unique episode IDs in Pinecone.")
        return pinecone_episode_ids

    except Exception as e:
        logger.error(f"An error occurred while getting IDs from Pinecone: {e}", exc_info=True)
        return set()

def main():
    """Main function to run the cross-check."""
    logger.info("--- Starting BigQuery vs. Pinecone Cross-Check ---")
    
    bq_episodes = get_bq_episode_ids()
    pinecone_episodes = get_pinecone_episode_ids()

    if not bq_episodes and not pinecone_episodes:
        logger.error("Could not retrieve episode IDs from either BigQuery or Pinecone. Aborting.")
        return

    logger.info("\n--- CROSS-CHECK REPORT ---")
    print(f"Unique episodes in BigQuery:    {len(bq_episodes)}")
    print(f"Unique episodes in Pinecone:     {len(pinecone_episodes)}")
    
    missing_from_pinecone = bq_episodes - pinecone_episodes
    if not missing_from_pinecone:
        print("\n✅ SUCCESS: All episodes from BigQuery are present in Pinecone.")
    else:
        print(f"\n🚨 WARNING: Found {len(missing_from_pinecone)} episodes in BigQuery that are MISSING from Pinecone:")
        for episode_id in sorted(list(missing_from_pinecone)):
            print(f"  - {episode_id}")

    missing_from_bq = pinecone_episodes - bq_episodes
    if missing_from_bq:
        print(f"\nINFO: Found {len(missing_from_bq)} episodes in Pinecone that are not in the BigQuery table (could be from old data):")
        for episode_id in sorted(list(missing_from_bq)):
            print(f"  - {episode_id}")

    logger.info("\n--- Cross-Check Complete ---")

if __name__ == "__main__":
    main() 