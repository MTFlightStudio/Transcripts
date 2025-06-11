import os
import pinecone
from tqdm import tqdm
import logging
from collections import defaultdict
import hashlib
import json

# Set up logging
logging.basicConfig(level=logging.INFO)

# Get API key from environment variable
PINECONE_API_KEY = "96e25114-0519-49c7-855d-c5728c49a33f"

# Check if API key is set
if not PINECONE_API_KEY:
    raise ValueError("Pinecone API key not found. Please set the PINECONE_API_KEY environment variable.")

# Initialize Pinecone
pc = pinecone.Pinecone(api_key=PINECONE_API_KEY)

# Connect to the Pinecone index
index_name = "youtube-transcripts-embeddings-speaker9"
index = pc.Index(index_name)

def get_episode_ids():
    with open("video_ids.txt", "r") as f:
        return [line.strip() for line in f if line.strip()]

def get_vectors_for_episode(TARGET_EPISODE_ID):
    all_vectors = []
    batch_size = 1000
    next_page_token = None
    
    while True:
        results = index.query(
            vector=[0] * 1536,
            top_k=batch_size,
            include_metadata=True,
            filter={"episode_id": TARGET_EPISODE_ID},
            next_page_token=next_page_token
        )
        filtered_vectors = [v for v in results['matches'] if v['metadata'].get('episode_id') == TARGET_EPISODE_ID]
        all_vectors.extend(filtered_vectors)
        logging.info(f"Fetched batch: {len(filtered_vectors)} vectors")
        
        if 'next_page_token' not in results or not results['next_page_token']:
            break
        next_page_token = results['next_page_token']
    
    logging.info(f"Total vectors fetched for episode {TARGET_EPISODE_ID}: {len(all_vectors)}")
    return all_vectors

def hash_text(text):
    return hashlib.md5(text.encode()).hexdigest()

def find_duplicates(vectors):
    chunk_dict = defaultdict(list)
    for vector in vectors:
        metadata = vector['metadata']
        key = (
            metadata.get('chunk_start_time', ''),
            hash_text(metadata.get('chunk_with_speaker', ''))
        )
        chunk_dict[key].append((vector['id'], metadata))
    
    duplicates = {key: ids for key, ids in chunk_dict.items() if len(ids) > 1}
    return duplicates

def save_duplicate_examples(duplicates, episode_id, num_examples=10):
    examples = []
    for key, id_metadata_pairs in list(duplicates.items())[:num_examples]:
        example = {
            "chunk_start_time": key[0],
            "duplicates": [
                {
                    "id": id,
                    "metadata": metadata
                } for id, metadata in id_metadata_pairs
            ]
        }
        examples.append(example)
    
    with open(f"duplicate_examples_{episode_id}.json", "w") as f:
        json.dump(examples, f, indent=2)
    
    logging.info(f"Saved {len(examples)} duplicate examples to duplicate_examples_{episode_id}.json")

def delete_duplicates(duplicates):
    total_deleted = 0
    for key, id_metadata_pairs in tqdm(duplicates.items(), desc="Deleting duplicates"):
        ids_to_delete = [id for id, _ in id_metadata_pairs[1:]]
        index.delete(ids=ids_to_delete)
        total_deleted += len(ids_to_delete)
    return total_deleted

def main():
    episode_ids = get_episode_ids()
    episodes_with_zero_vectors = []
    
    excluded_episode_ids = [
        "WJcIfLUR6gg", "gelTkUE_YdE", "ZTA2RJ6dEFc", "OpANpfEjNw8",
        "v9VtjcRaRWc", "ls9be-9C-uk", "_wSO42jYP6Y", "7zn7WqAyhiU"
    ]
    
    for current_episode_id in tqdm(episode_ids, desc="Processing episodes"):
        if current_episode_id in excluded_episode_ids:
            logging.info(f"Skipping excluded episode {current_episode_id}")
            continue
        
        logging.info(f"\nProcessing episode {current_episode_id}")
        logging.info(f"Fetching vectors for episode {current_episode_id} from Pinecone...")
        episode_vectors = get_vectors_for_episode(current_episode_id)

        if not episode_vectors:
            episodes_with_zero_vectors.append(current_episode_id)
            logging.info(f"No vectors found for episode {current_episode_id}.")
            continue

        logging.info("Finding duplicates...")
        duplicates = find_duplicates(episode_vectors)
        logging.info(f"Number of chunks with duplicates: {len(duplicates)}")

        if duplicates:
            total_duplicates = sum(len(ids) - 1 for ids in duplicates.values())
            logging.info(f"Total number of duplicate vectors: {total_duplicates}")
            logging.info(f"Percentage of duplicates: {(total_duplicates / len(episode_vectors)) * 100:.2f}%")

            save_duplicate_examples(duplicates, current_episode_id)

            confirm = input(f"Do you want to proceed with deleting the duplicates for episode {current_episode_id}? (yes/no): ")
            if confirm.lower() == 'yes':
                deleted_count = delete_duplicates(duplicates)
                logging.info(f"Deleted {deleted_count} duplicate vectors for episode {current_episode_id}.")
            else:
                logging.info(f"Deletion cancelled for episode {current_episode_id}.")
        else:
            logging.info(f"No duplicates found for episode {current_episode_id}.")

    if episodes_with_zero_vectors:
        logging.info("\nEpisodes with zero vectors (excluding the specified list):")
        for episode_id in episodes_with_zero_vectors:
            youtube_link = f"https://www.youtube.com/watch?v={episode_id}"
            logging.info(youtube_link)
        logging.info(f"Total episodes with zero vectors: {len(episodes_with_zero_vectors)}")
    else:
        logging.info("\nAll processed episodes have vectors present in the database.")

if __name__ == "__main__":
    main()