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
INDEX_NAMES = [
    "youtube-transcripts-embeddings-no-speaker11",
    "youtube-transcripts-embeddings-speaker11"
]

# Queries designed to fetch new and potentially older data
QUERIES = {
    "new_data_query": "Jimmy Fallon",
    "old_data_query": "consciousness" 
}

# Number of sample results to fetch for each query
TOP_K = 2

def get_embedding(text, model="text-embedding-ada-002"):
    """Generates an embedding for the given text using OpenAI."""
    try:
        response = client.embeddings.create(input=[text], model=model)
        return response.data[0].embedding
    except Exception as e:
        print(f"Error getting embedding for '{text}': {e}")
        return None

def inspect_metadata():
    """
    Queries each Pinecone index with predefined queries and prints the metadata
    of the results, including the data types of each field.
    """
    for index_name in INDEX_NAMES:
        print("\n" + "="*80)
        print(f"🔬 INSPECTING INDEX: {index_name}")
        print("="*80)
        
        try:
            index = pc.Index(index_name)
        except Exception as e:
            print(f"❌ ERROR: Could not connect to index '{index_name}'. Skipping. Error: {e}")
            continue

        for query_type, query_text in QUERIES.items():
            print(f"\n--- Querying for '{query_text}' (as {query_type}) ---")
            
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
                print(f"❌ ERROR: Failed to query index '{index_name}'. Error: {e}")
                continue

            if not results['matches']:
                print("  -> No results found for this query.")
                continue

            for i, match in enumerate(results['matches']):
                print(f"\n  [Result {i+1}]")
                print(f"  - ID: {match.get('id', 'N/A')}")
                print(f"  - Score: {match.get('score', 0.0):.4f}")
                
                metadata = match.get('metadata', {})
                if not metadata:
                    print("  - Metadata: None")
                    continue
                
                print("  - Metadata Fields & Types:")
                for key, value in metadata.items():
                    value_type = type(value).__name__
                    print(f"    - {key:<30} | Type: {value_type:<10} | Value: {str(value)[:100]}") # Truncate long values
    
    print("\n" + "="*80)
    print("✅ Inspection Complete.")
    print("="*80)


if __name__ == "__main__":
    inspect_metadata() 