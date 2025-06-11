import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

def generate_dummy_data(
    start_date="2025-02-01",
    days=30,
    platforms=("Spotify", "Apple", "YouTube"),
    output_csv="podcast_performance.csv"
):
    """
    Generate dummy data for podcast performance metrics.
    """

    # Convert start_date string to datetime
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    
    # We'll collect rows in a list of dicts
    data_rows = []

    # Suppose we have 3 episodes (just for variety)
    episode_ids = [f"E{str(i).zfill(3)}" for i in range(1, 4)]

    for day_offset in range(days):
        current_date = start_dt + timedelta(days=day_offset)
        
        for platform in platforms:
            # Randomly pick an episode for the day
            # In reality, you might have multiple episodes live on the same day
            episode_id = np.random.choice(episode_ids)

            # Generate some random numbers for downloads, views, watch_time
            # Adjust ranges as desired
            downloads = np.random.randint(50, 500) if platform in ["Spotify", "Apple"] else 0
            views = np.random.randint(100, 2000) if platform == "YouTube" else 0
            watch_time_minutes = np.random.randint(0, 3000)

            data_rows.append({
                "date": current_date.strftime("%Y-%m-%d"),
                "platform": platform,
                "episode_id": episode_id,
                "downloads": downloads,
                "views": views,
                "watch_time_minutes": watch_time_minutes,
            })

    # Convert to a DataFrame
    df = pd.DataFrame(data_rows)

    # Shuffle the rows so it's not strictly by date+platform
    df = df.sample(frac=1, random_state=42).reset_index(drop=True)
    
    # Save to CSV
    df.to_csv(output_csv, index=False)
    print(f"Dummy data saved to {output_csv}")

if __name__ == "__main__":
    generate_dummy_data()
