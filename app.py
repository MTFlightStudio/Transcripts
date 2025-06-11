guest_name = extract_guest_name(query)

# Define the base filter. An empty filter matches all documents.
base_filter = {}

# We won't add date filter to Pinecone query - will filter results after retrieval
if start_date and end_date:
    start_date_str = start_date.strftime('%Y-%m-%d') 