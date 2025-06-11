import os
import pinecone
from openai import OpenAI
from dotenv import load_dotenv
from pathlib import Path
import pprint

# --- CONFIGURATION ---
# Load environment variables from config.env
script_dir = Path(__file__).parent.resolve()
config_path = script_dir / 'config.env'
load_dotenv(dotenv_path=config_path)

# Initialize Pinecone and OpenAI clients
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if not PINECONE_API_KEY or not OPENAI_API_KEY:
    raise ValueError("PINECONE_API_KEY and OPENAI_API_KEY must be set in your config.env file.")

try:
    pc = pinecone.Pinecone(api_key=PINECONE_API_KEY)
    client = OpenAI(api_key=OPENAI_API_KEY)
    print("Successfully connected to Pinecone and OpenAI.")
except Exception as e:
    print(f"Error initializing clients: {e}")
    exit()

# --- SCRIPT PARAMETERS ---

# ----------------------------------------------------------------------------------
# CHOOSE WHICH INDEX TO QUERY: SET TO `False` for 'no-speaker', `True` for 'speaker'
# ----------------------------------------------------------------------------------
USE_SPEAKER_INDEX = True
# ----------------------------------------------------------------------------------

# Define the v2 index names
INDEX_NAME_NO_SPEAKER = "youtube-transcripts-embeddings-no-speaker-v2"
INDEX_NAME_SPEAKER = "youtube-transcripts-embeddings-speaker-v2"

# Set the index name based on the flag
INDEX_NAME = INDEX_NAME_SPEAKER if USE_SPEAKER_INDEX else INDEX_NAME_NO_SPEAKER

# List of 4 random queries to test against the index
TEST_QUERIES = [
    "impact of social media on mental health",
    "the future of artificial intelligence",
    "intermittent fasting and its benefits",
    "stoicism as a philosophy for modern life"
]

# Number of results to fetch for each query
TOP_K = 10

def get_embedding(text, model="text-embedding-ada-002"):
    """Generates an embedding for the given text using OpenAI."""
    try:
        response = client.embeddings.create(input=[text], model=model)
        return response.data[0].embedding
    except Exception as e:
        print(f"Error getting embedding for '{text}': {e}")
        return None

def run_queries():
    """
    Queries the selected Pinecone index with the list of test queries and prints the results.
    """
    print("\n" + "="*80)
    print(f"🔬 QUERYING INDEX: {INDEX_NAME}")
    print("="*80)
    
    try:
        index = pc.Index(INDEX_NAME)
        print(f"Successfully connected to index '{INDEX_NAME}'.")
    except Exception as e:
        print(f"❌ ERROR: Could not connect to index '{INDEX_NAME}'. Exiting. Error: {e}")
        return

    for query_text in TEST_QUERIES:
        print(f"\n\n--- Testing Query: '{query_text}' ---")
        
        embedding = get_embedding(query_text)
        if not embedding:
            continue

        try:
            results = index.query(
                vector=embedding,
                top_k=TOP_K,
                include_metadata=True
            )
        except Exception as e:
            print(f"❌ ERROR: Failed to query index '{INDEX_NAME}'. Error: {e}")
            continue

        if not results['matches']:
            print("  -> No results found for this query.")
            continue

        intensity_scores = []
        for i, match in enumerate(results['matches']):
            metadata = match.get('metadata', {})
            text_field = 'chunk_with_speaker' if USE_SPEAKER_INDEX else 'chunk'
            chunk_text = metadata.get(text_field, "N/A")
            
            # Explicitly get the intensity score to print it
            intensity_score = metadata.get('intensityScoreNormalized', 'Not Available')

            print(f"\n  [Result {i+1}] - Score: {match.get('score', 0.0):.4f}")
            print(f"  - Episode: {metadata.get('episode_name', 'N/A').replace('_', ' ')}")
            print(f"  - Date: {metadata.get('release_date', 'N/A')}")
            print(f"  - Intensity Score: {intensity_score}")
            print(f"  - Text: \"{chunk_text.strip()}\"")

            # Collect intensity scores for averaging
            if intensity_score != 'Not Available':
                try:
                    intensity_scores.append(float(intensity_score))
                except (ValueError, TypeError):
                    pass # Ignore if score is not a valid number
        
        # Calculate and print the average intensity score
        if intensity_scores:
            average_score = sum(intensity_scores) / len(intensity_scores)
            print(f"\n  --------------------------------------------------")
            print(f"  📊 Average Intensity Score for '{query_text}': {average_score:.4f}")
            print(f"  --------------------------------------------------")
        else:
            print(f"\n  -> No valid intensity scores found for this query to average.")

            
    print("\n" + "="*80)
    print("✅ Querying Complete.")
    print("="*80)


if __name__ == "__main__":
    run_queries() 