import os
import logging
import yt_dlp as youtube_dl
import whisper
from time import sleep

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to download audio from a YouTube video
def download_audio_from_youtube(video_id, output_path="audio"):
    logging.info(f"Starting download for video ID: {video_id}")
    try:
        video_url = f"https://www.youtube.com/watch?v={video_id}"
        logging.info(f"Constructed YouTube URL: {video_url}")
        
        ydl_opts = {
            'format': 'bestaudio/best',
            'outtmpl': os.path.join(output_path, '%(id)s.%(ext)s'),
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '192',
            }],
            'ffmpeg_location': '/opt/homebrew/bin'  # Update this path to where ffmpeg is installed
        }
        
        with youtube_dl.YoutubeDL(ydl_opts) as ydl:
            ydl.download([video_url])
            info_dict = ydl.extract_info(video_url, download=False)
            audio_file = os.path.join(output_path, f"{info_dict['id']}.mp3")
        
        logging.info(f"Downloaded and saved audio for video ID: {video_id} to {audio_file}")
        return audio_file
    except Exception as e:
        logging.error(f"Error downloading {video_id}: {e}", exc_info=True)
        return None

# Function to transcribe a single audio file
def transcribe_single_audio(audio_file):
    logging.info(f"Starting transcription for audio file: {audio_file}")
    try:
        model = whisper.load_model("base")
        result = model.transcribe(audio_file)
        logging.info(f"Completed transcription for audio file: {audio_file}")
        return result['text']
    except Exception as e:
        logging.error(f"Error transcribing {audio_file}: {e}", exc_info=True)
        return None

# Function to download and transcribe videos sequentially
def download_and_transcribe(video_ids, output_file, output_path="audio"):
    logging.info(f"Starting sequential download and transcription for {len(video_ids)} videos")

    for video_id in video_ids:
        audio_file = download_audio_with_retry(video_id, output_path)
        if audio_file:
            transcription = transcribe_single_audio(audio_file)
            if transcription:
                save_transcription(video_id, transcription, output_file)
                logging.info(f"Transcription for video ID {video_id} completed and saved")
    
    logging.info("All downloads and transcriptions completed")

# Function to download audio with retry logic
def download_audio_with_retry(video_id, output_path="audio", retries=3, delay=5):
    for attempt in range(retries):
        audio_file = download_audio_from_youtube(video_id, output_path)
        if audio_file:
            return audio_file
        logging.info(f"Retrying download for video ID: {video_id} (Attempt {attempt + 1}/{retries})")
        sleep(delay)
    logging.error(f"Failed to download video ID: {video_id} after {retries} attempts")
    return None

# Load the list of video IDs from the temporary file
def load_video_ids(temp_file):
    logging.info(f"Loading video IDs from {temp_file}")
    with open(temp_file, 'r') as f:
        video_ids = [line.strip() for line in f]
    logging.info(f"Loaded {len(video_ids)} video IDs")
    return video_ids

# Save a single transcription to a file
def save_transcription(video_id, transcription, output_file):
    logging.info(f"Saving transcription for video ID {video_id} to {output_file}")
    try:
        with open(output_file, 'a') as f:
            f.write(f"Video ID: {video_id}\n")
            f.write(f"Transcription:\n{transcription}\n")
            f.write("-" * 80 + "\n")
        logging.info(f"Transcription for video ID {video_id} saved to {output_file}")
    except Exception as e:
        logging.error(f"Error saving transcription for video ID {video_id}: {e}", exc_info=True)

if __name__ == "__main__":
    # Path to the temp file where video_ids are listed
    temp_file = "temp_video_ids_test.txt"  # Adjust the path as necessary
    output_file = "transcriptions.txt"  # Output file to save the transcriptions
    
    # Load the list of video IDs
    video_ids = load_video_ids(temp_file)
    
    # Download and transcribe all videos
    download_and_transcribe(video_ids, output_file, output_path="audio")
    
    logging.info(f"Transcription process completed. Transcriptions saved to {output_file}")