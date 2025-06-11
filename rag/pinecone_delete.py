import pinecone

# Initialize the Pinecone client
PINECONE_API_KEY = "96e25114-0519-49c7-855d-c5728c49a33f"
pc = pinecone.Pinecone(api_key=PINECONE_API_KEY)

# Get the index
index_name = "youtube-transcripts-embeddings-no-speaker5"
index = pc.Index(index_name)

# List of episode IDs you want to delete
episode_ids = [
    "WJcIfLUR6gg",
    "gelTkUE_YdE",
    "ZTA2RJ6dEFc",
    "OpANpfEjNw8",
    "v9VtjcRaRWc",
    "ls9be-9C-uk",
    "_wSO42jYP6Y",
    "7zn7WqAyhiU"
]

# Function to fetch vector IDs for a given episode
def fetch_vector_ids(episode_id, batch_size=1000):
    vector_ids = []
    next_page_token = None
    while True:
        response = index.query(
            vector=[0] * 1536,  # Assuming 1536 is your vector dimension
            top_k=batch_size,
            include_metadata=True,
            filter={"episode_id": episode_id},
            next_page_token=next_page_token
        )
        vector_ids.extend([match.id for match in response.matches])
        next_page_token = response.next_page_token
        if not next_page_token:
            break
    return vector_ids

# Loop over each episode ID, fetch vector IDs, and delete them
for episode_id in episode_ids:
    print(f"Processing episode ID: {episode_id}")
    vector_ids = fetch_vector_ids(episode_id)
    if vector_ids:
        print(f"Deleting {len(vector_ids)} vectors for episode {episode_id}")
        index.delete(ids=vector_ids)
    else:
        print(f"No vectors found for episode {episode_id}")

print("Deletion process completed.")
