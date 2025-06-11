from dotenv import load_dotenv
import requests
import json
import os
import logging
import time
from pathlib import Path
import argparse

# --- CONFIGURATION ---
# Load environment variables from .env file
# Build a path to config.env relative to this script's location for robustness
script_dir = Path(__file__).parent.resolve()
config_path = script_dir / 'config.env'
load_dotenv(dotenv_path=config_path)

# Get SAPISIDHASH from environment variables
SAPISIDHASH = os.getenv("SAPISIDHASH")

# The proxy server URL that the Docker container runs
API_BASE_URL = "http://localhost:8081/videos"

# --- END CONFIGURATION ---

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_video_details(video_id):
    """
    Fetches video details via the local proxy server.
    NOTE: This requires the Docker container proxy to be running.
    """
    if not SAPISIDHASH:
        logging.error("SAPISIDHASH not found. Cannot make request.")
        return None
        
    url = f"{API_BASE_URL}?part=mostReplayed,snippet,statistics&id={video_id}&SAPISIDHASH={SAPISIDHASH}"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        # The proxy often returns non-JSON text before the actual data
        json_start = response.text.find('{')
        if json_start == -1:
            logging.error(f"No JSON object found in response for video {video_id}")
            return None

        clean_json_text = response.text[json_start:]
        data = json.loads(clean_json_text)

        if 'items' in data and len(data['items']) > 0:
            return data['items'][0]  # Return the first item
        else:
            logging.warning(f"Response for {video_id} contained no 'items'.")
            return None
            
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to retrieve data for video {video_id} via proxy: {e}")
        return None
    except json.JSONDecodeError:
        logging.error(f"Failed to parse JSON response for video {video_id} from proxy")
        return None

def main(video_id, output_file):
    if not SAPISIDHASH:
        logging.error("SAPISIDHASH is not set in the environment variables. Please set it in config.env.")
        return

    logging.info(f"--- Running Test: Get Most Replayed Segments for Video ID: {video_id} ---")
    
    details = get_video_details(video_id)
    
    if not details:
        logging.error(f"Could not retrieve details for video {video_id}.")
        return

    video_title = details.get('snippet', {}).get('title', 'N/A')
    logging.info(f"Successfully retrieved details for video: {video_title}")
    
    most_replayed_data = details.get('mostReplayed', {})
    markers = most_replayed_data.get('markers', [])

    # Prepare the data for JSON output
    output_data = {
        "videoId": video_id,
        "videoTitle": video_title,
        "mostReplayedMarkers": markers,
        "videoDetails": details 
    }
    
    # Remove mostReplayed from the detailed view to avoid duplication
    if 'mostReplayed' in output_data['videoDetails']:
        del output_data['videoDetails']['mostReplayed']

    # Write to the output file
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        logging.info(f"Successfully wrote MRM data to {output_file}")
    except IOError as e:
        logging.error(f"Failed to write to file {output_file}: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch 'Most Replayed' data for a single YouTube video and save it to a JSON file.")
    parser.add_argument("video_id", type=str, help="The YouTube video ID to test.")
    parser.add_argument(
        "-o", "--output", 
        type=str, 
        default=None,
        help="Path to the output JSON file. Defaults to 'mrm_output_[video_id].json'."
    )
    args = parser.parse_args()

    output_file_path = args.output
    if not output_file_path:
        output_file_path = f"mrm_output_{args.video_id}.json"
    
    main(args.video_id, output_file_path) 