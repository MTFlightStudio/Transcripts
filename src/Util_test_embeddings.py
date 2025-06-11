import os
from openai import OpenAI
from dotenv import load_dotenv
import numpy as np
from pathlib import Path

# --- CONFIGURATION ---
# Load environment variables from config.env, which is the standard for this project
script_dir = Path(__file__).parent.resolve()
config_path = script_dir / 'config.env'
load_dotenv(dotenv_path=config_path)

# Initialize OpenAI
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise ValueError("OPENAI_API_KEY is not set in your config.env file.")
client = OpenAI(api_key=OPENAI_API_KEY)

def get_embedding(text):
    """Get embedding for text."""
    response = client.embeddings.create(
        input=text,
        model="text-embedding-ada-002"
    )
    return response.data[0].embedding

def cosine_similarity(a, b):
    """Calculate cosine similarity between two vectors."""
    return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))

# Example chunk content
chunk_content = "And then we did a Saturday Night Live sketch about the presidential debate. It was hilarious, everyone was laughing at the impressions."

# Two versions of the same content
chunk_only = chunk_content
chunk_with_speaker = f"Jimmy Fallon: {chunk_content}"

# Get embeddings
print("Getting embeddings...")
embedding_chunk = get_embedding(chunk_only)
embedding_speaker = get_embedding(chunk_with_speaker)

# Test queries
queries = [
    "Saturday Night Live",
    "Jimmy Fallon",
    "SNL sketch comedy",
    "late night talk show host"
]

print("\nCosine Similarity Scores:")
print("=" * 60)

for query in queries:
    query_embedding = get_embedding(query)
    
    sim_chunk = cosine_similarity(query_embedding, embedding_chunk)
    sim_speaker = cosine_similarity(query_embedding, embedding_speaker)
    
    print(f"\nQuery: '{query}'")
    print(f"  Similarity with chunk (no speaker):     {sim_chunk:.4f}")
    print(f"  Similarity with chunk (with speaker):   {sim_speaker:.4f}")
    print(f"  Difference:                             {abs(sim_chunk - sim_speaker):.4f}")
    
    if sim_chunk > sim_speaker:
        print(f"  ✅ Better match: No-speaker version")
    else:
        print(f"  ✅ Better match: Speaker version")