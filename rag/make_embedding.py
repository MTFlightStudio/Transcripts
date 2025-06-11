import os
from openai import OpenAI
import logging
import json

# Set up logging
logging.basicConfig(level=logging.INFO)

# Get API key from environment variable
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise ValueError("No OpenAI API key found in environment variables. Please set the OPENAI_API_KEY.")

# Initialize OpenAI client with API key
client = OpenAI(api_key=OPENAI_API_KEY)

def get_embeddings(text):
    try:
        response = client.embeddings.create(input=[text], model="text-embedding-ada-002")
        return response.data[0].embedding
    except Exception as e:
        logging.error(f"Error creating embedding: {e}")
        return None

if __name__ == "__main__":
    sample_text = " I shot up a full foot in height, but was very, very skinny at that point."
    embedding = get_embeddings(sample_text)
    if embedding:
        with open("embedding.json", "w") as f:
            json.dump(embedding, f)
        print("Embedding saved to embedding.json")
    else:
        print("Failed to get embedding.")