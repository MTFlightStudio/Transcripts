guest_name = extract_guest_name(query)

# Conditionally apply the start_time filter.
# If a guest name is detected, we search the whole transcript (including intros).
# Otherwise, we skip the first 90 seconds to avoid trailers.
if guest_name:
    base_filter = {}
    print("Guest name detected, searching full transcript.")
else:
    base_filter = {
        "start_time": {"$gte": 90}
    }
    print("No guest name detected, skipping first 90 seconds.")

# We won't add date filter to Pinecone query - will filter results after retrieval
if start_date and end_date:
    start_date_str = start_date.strftime('%Y-%m-%d') 