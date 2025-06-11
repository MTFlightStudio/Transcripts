import json
import argparse
import numpy as np

def format_ms_to_min_sec(ms):
    """Converts milliseconds to a MM:SS string format."""
    total_seconds = int(ms / 1000)
    minutes = total_seconds // 60
    seconds = total_seconds % 60
    return f"{minutes:02d}:{seconds:02d}"

def analyze_mrm_data(file_path):
    """Analyzes the MRM JSON output file and prints a summary."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
        return
    except json.JSONDecodeError:
        print(f"Error: The file '{file_path}' is not a valid JSON file.")
        return

    video_title = data.get('videoTitle', 'N/A')
    video_id = data.get('videoId', 'N/A')
    markers = data.get('mostReplayedMarkers', [])

    print("--- MRM Analysis Report ---")
    print(f"Video Title: {video_title}")
    print(f"Video ID: {video_id}")
    print("---------------------------\n")

    if not markers:
        print("No 'mostReplayedMarkers' found in the file.")
        return

    print(f"Total Most Replayed Moments Found: {len(markers)}\n")
    print("Timestamp (MM:SS) | Intensity Score")
    print("------------------|-----------------")

    intensities = []
    for marker in markers:
        start_ms = marker.get('startMillis', 0)
        intensity = marker.get('intensityScoreNormalized', 0)
        intensities.append(intensity)
        
        timestamp_str = format_ms_to_min_sec(start_ms)
        
        print(f"{timestamp_str:^18}| {intensity:.4f}")

    # Summary Statistics
    if intensities:
        print("\n--- Intensity Score Summary ---")
        # The first marker at 0s often has an intensity of 1.0, which can skew the mean.
        # Let's exclude it from the summary stats if it exists and is exactly 1.0.
        filtered_intensities = [i for i, m in zip(intensities, markers) if m.get('startMillis', 0) > 0]
        if not filtered_intensities: # handle case where there's only one marker at 0s
             filtered_intensities = intensities

        avg_intensity = np.mean(filtered_intensities)
        max_intensity = np.max(filtered_intensities)
        min_intensity = np.min(filtered_intensities)
        median_intensity = np.median(filtered_intensities)

        print(f"Average Intensity (excluding 0s marker): {avg_intensity:.4f}")
        print(f"Median Intensity: {median_intensity:.4f}")
        print(f"Maximum Intensity: {max_intensity:.4f}")
        print(f"Minimum Intensity: {min_intensity:.4f}")
        print("-------------------------------\n")
        
        # Find the timestamp of the highest intensity peak (ignoring the 0s mark)
        peak_marker = max(
            (m for m in markers if m.get('startMillis', 0) > 0), 
            key=lambda x: x['intensityScoreNormalized'], 
            default=None
        )
        if peak_marker:
            peak_time = format_ms_to_min_sec(peak_marker['startMillis'])
            peak_intensity = peak_marker['intensityScoreNormalized']
            print(f"🔥 Peak Replay Moment (Highest Intensity):")
            print(f"   Timestamp: {peak_time}")
            print(f"   Intensity: {peak_intensity:.4f}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze a 'Most Replayed' JSON output file.")
    parser.add_argument("file_path", type=str, help="Path to the MRM JSON file to analyze.")
    args = parser.parse_args()
    
    analyze_mrm_data(args.file_path) 