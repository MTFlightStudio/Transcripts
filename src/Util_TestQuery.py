import os
import logging
from pinecone import Pinecone
from openai import OpenAI
from dotenv import load_dotenv
from pathlib import Path
import pandas as pd
from typing import List

# --- CONFIGURATION ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Load Environment Variables ---
script_dir = Path(__file__).parent.resolve()
config_path = script_dir / 'config.env'
load_dotenv(dotenv_path=config_path)

PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
INDEX_NAME = "youtube-transcripts-embeddings-no-speaker11"

# --- The Query to Test ---
TEST_QUERY = "jimmy fallon"
TOP_K = 5 # Number of results to fetch

def main():
    """Connects to Pinecone, runs a single query, and prints the results."""
    logger.info("--- Starting One-Off Pinecone Query Test ---")
    
    if not PINECONE_API_KEY or not OPENAI_API_KEY:
        logger.error("PINECONE_API_KEY and OPENAI_API_KEY must be set. Aborting.")
        return

    try:
        # --- Initialize Clients ---
        logger.info("Initializing OpenAI and Pinecone clients...")
        openai_client = OpenAI(api_key=OPENAI_API_KEY)
        pc = Pinecone(api_key=PINECONE_API_KEY)
        index = pc.Index(INDEX_NAME)
        logger.info(f"Successfully connected to Pinecone index '{INDEX_NAME}'.")

        # --- Create Embedding ---
        logger.info(f"Creating embedding for the query: '{TEST_QUERY}'...")
        query_embedding = openai_client.embeddings.create(
            input=[TEST_QUERY],
            model="text-embedding-ada-002"
        ).data[0].embedding
        logger.info("Embedding created successfully.")

        # --- Query Pinecone ---
        logger.info(f"Querying Pinecone for the top {TOP_K} results...")
        results = index.query(
            vector=query_embedding,
            top_k=TOP_K,
            include_metadata=True
        )
        logger.info("Query complete.")

        # --- Display Results ---
        if not results['matches']:
            logger.warning("Query returned no results.")
            return

        print("\n--- QUERY RESULTS ---")
        for i, match in enumerate(results['matches']):
            metadata = match.get('metadata', {})
            score = match['score']
            
            print(f"\n--- Result #{i+1} (Relevance Score: {score:.4f}) ---")
            
            # Print all metadata fields dynamically
            for key, value in metadata.items():
                # Format for better readability
                print(f"{key:<28}: {value}")
        
        print("\n--- End of Report ---")

    except Exception as e:
        logger.error(f"An error occurred during the test: {e}", exc_info=True)


if __name__ == "__main__":
    main() 