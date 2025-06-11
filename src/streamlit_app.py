import os
import pinecone
from openai import OpenAI
import datetime
from datetime import date, timedelta
import streamlit as st
import spacy
import asyncio
from collections import deque
from concurrent.futures import ThreadPoolExecutor
import warnings
import dotenv
import os

dotenv.load_dotenv()

# Suppress Streamlit warnings about missing ScriptRunContext
warnings.filterwarnings("ignore", message=".*missing ScriptRunContext.*")

# Initialize Pinecone and OpenAI clients
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Cache Pinecone client
@st.cache_resource
def get_pinecone_client():
    return pinecone.Pinecone(api_key=PINECONE_API_KEY)

pc = get_pinecone_client()
index = pc.Index("youtube-transcripts-embeddings-no-speaker11")
guest_index = pc.Index("youtube-transcripts-embeddings-speaker11")

# Cache OpenAI client
@st.cache_resource
def get_openai_client():
    return OpenAI(api_key=OPENAI_API_KEY)

client = get_openai_client()

# Load SpaCy model
@st.cache_resource
def load_spacy_model():
    return spacy.load("en_core_web_sm")

nlp = load_spacy_model()

@st.cache_data(ttl=3600)
def enhanced_query(query):
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": "You are a query enhancer for a podcast transcript search system. Expand the given query to include related terms, synonyms, and contextual information relevant to podcasts. Focus on creating a comprehensive query that captures the user's intent and related concepts. Return only the enhanced query without any additional text but try to keep it as short as possible."},
            {"role": "user", "content": f"Enhance this query for better search results: {query}"}
        ]
    )
    enhanced_query_text = response.choices[0].message.content.strip()
    print(f"Enhanced Query: {enhanced_query_text}")
    return enhanced_query_text

@st.cache_data(ttl=3600)
def extract_guest_name(query):
    doc = nlp(query)
    for ent in doc.ents:
        if ent.label_ == "PERSON":
            return ent.text
    print("No name specified")
    return None

@st.cache_data(ttl=3600)
def get_query_embedding(query):
    return client.embeddings.create(input=query, model="text-embedding-ada-002").data[0].embedding
    
def semantic_search_with_context(query, top_k=25, context_window=5, start_date=None, end_date=None):
    """
    Perform semantic search with date filtering applied after retrieval
    """
    enhanced_query_text = enhanced_query(query)
    query_embedding = get_query_embedding(enhanced_query_text)
    
    guest_name = extract_guest_name(query)
    
    # Define the base filter - only keep the start_time filter as mandatory
    base_filter = {
        "start_time": {"$gte": 90}  # Filter for chunks with start_time >= 90 seconds
    }
    
    # We won't add date filter to Pinecone query - will filter results after retrieval
    if start_date and end_date:
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        print(f"Will apply date filter after retrieval: {start_date_str} to {end_date_str}")
    
    try:
        if guest_name:
            results = guest_index.query(
                vector=query_embedding,
                filter=base_filter,
                top_k=top_k * 2,  # Increased since we'll filter results later
                include_metadata=True
            )
            print("Querying speaker db")
        else:
            results = index.query(
                vector=query_embedding,
                filter=base_filter,
                top_k=top_k * 2,  # Increased since we'll filter results later
                include_metadata=True
            )
            print("Querying no speaker db")
        
        # Apply date filtering post-retrieval
        if start_date and end_date:
            filtered_matches = []
            for match in results['matches']:
                try:
                    # Try to parse the date from the result
                    result_date_str = match['metadata'].get('release_date')
                    if not result_date_str:
                        match['in_date_range'] = False
                        filtered_matches.append(match)
                        continue
                        
                    # Handle different date formats ('YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS')
                    result_date_str_cleaned = result_date_str.split(' ')[0]
                    result_date = datetime.datetime.strptime(result_date_str_cleaned, '%Y-%m-%d').date()

                    if start_date <= result_date <= end_date:
                        match['in_date_range'] = True
                        filtered_matches.append(match)
                    else:
                        match['in_date_range'] = False
                        # Only include out-of-range results if we don't have enough in-range ones
                        if len(filtered_matches) < top_k // 2:
                            filtered_matches.append(match)
                except (ValueError, KeyError) as e:
                    print(f"Error parsing date: {e}")
                    match['in_date_range'] = False
                    filtered_matches.append(match)
            
            # Replace the original matches with filtered ones
            results['matches'] = filtered_matches[:top_k]
        else:
            # If no date filter, mark all results as in range
            for match in results['matches']:
                match['in_date_range'] = True
                
    except Exception as e:
        print(f"Error in Pinecone query: {e}")
        # Retry without any complex filters
        try:
            if guest_name:
                results = guest_index.query(
                    vector=query_embedding,
                    top_k=top_k,
                    include_metadata=True
                )
            else:
                results = index.query(
                    vector=query_embedding,
                    top_k=top_k,
                    include_metadata=True
                )
            
            # Mark all results as out of range in fallback mode
            for match in results['matches']:
                match['in_date_range'] = False
        except Exception as e2:
            print(f"Secondary error in Pinecone query: {e2}")
            return []  # Return empty list if all queries fail
    
    # Process results as before
    context_results = []
    for match in results['matches']:
        # Safely get metadata values with defaults
        metadata = match.get('metadata', {})
        episode_id = metadata.get('episode_id', 'unknown')
        start_time = metadata.get('start_time', 0)
        in_date_range = match.get('in_date_range', True)
        
        # Get nearby chunks for context
        try:
            nearby_filter = {
                "episode_id": {"$eq": episode_id},
                "start_time": {"$gte": float(start_time), "$lte": float(start_time) + 60}
            }
            
            nearby_chunks = (guest_index if guest_name else index).query(
                vector=query_embedding,
                filter=nearby_filter,
                top_k=15,
                include_metadata=True
            )
        except Exception as e:
            print(f"Error getting nearby chunks: {e}")
            # Fallback to simpler query
            try:
                nearby_chunks = (guest_index if guest_name else index).query(
                    vector=query_embedding,
                    filter={"episode_id": {"$eq": episode_id}},
                    top_k=15,
                    include_metadata=True
                )
            except:
                # If all fails, create empty nearby chunks
                nearby_chunks = {"matches": []}
        
        # Combine and sort chunks by start_time and chunk_index
        try:
            combined_chunks = sorted(
                nearby_chunks['matches'], 
                key=lambda x: (
                    float(x['metadata'].get('start_time', 0)), 
                    int(x['metadata'].get('chunk_index', 0))
                )
            )
        except Exception as e:
            print(f"Error sorting chunks: {e}")
            combined_chunks = nearby_chunks['matches']
        
        # Aggregate chunks with the same start_time
        aggregated_chunks = deque()
        current_chunk = None
        
        for chunk in combined_chunks:
            chunk_metadata = chunk.get('metadata', {})
            chunk_start_time = chunk_metadata.get('start_time', 0)
            
            if current_chunk and chunk_start_time == current_chunk.get('start_time', 0):
                current_chunk['chunk'] += "\n\n" + chunk_metadata.get('chunk', '')
                current_chunk['chunk_with_speaker'] += "\n\n" + chunk_metadata.get('chunk_with_speaker', '')
            else:
                if current_chunk:
                    aggregated_chunks.append(current_chunk)
                current_chunk = {
                    "speaker": chunk_metadata.get('speaker', 'Unknown'),
                    "chunk": chunk_metadata.get('chunk', ''),
                    "chunk_with_speaker": chunk_metadata.get('chunk_with_speaker', ''),
                    "start_time": chunk_start_time
                }
        
        if current_chunk:
            aggregated_chunks.append(current_chunk)
        
        # Create result with safe access to all fields
        context_result = {
            "question": metadata.get('chunk', ''),
            'chunk_with_speaker': metadata.get('chunk_with_speaker', ''),
            "context": list(aggregated_chunks),
            "speaker": metadata.get('speaker', 'Unknown'),
            "guest_name": metadata.get('guest_name', 'Unknown'),
            "episode_name": metadata.get('episode_name', 'Unknown'),
            "start_time": start_time,
            "intensity_score": metadata.get('intensityScoreNormalized', 0),
            "episode_id": episode_id,
            "relevance_score": match.get('score', 0),
            "release_date": metadata.get('release_date', ''),
            "in_date_range": in_date_range,
            'views': metadata.get('views', 0),
            'estimatedMinutesWatched': metadata.get('estimatedMinutesWatched', 0),
            'averageViewDuration': metadata.get('averageViewDuration', 0),
            'averageViewPercentage': metadata.get('averageViewPercentage', 0),
            'subscribersGained': metadata.get('subscribersGained', 0),
            'subscribersLost': metadata.get('subscribersLost', 0),
            'likes': metadata.get('likes', 0),
            'dislikes': metadata.get('dislikes', 0),
            'comments': metadata.get('comments', 0),
            'shares': metadata.get('shares', 0),
            'estimatedRevenue': metadata.get('estimatedRevenue', 0),
            'intensityScoreNormalized': metadata.get('intensityScoreNormalized', 0),
            'relativeRetentionPerformance': metadata.get('relativeRetentionPerformance', 0),
            'audienceWatchRatio': metadata.get('audienceWatchRatio', 0)
        }
        
        context_results.append(context_result)
    
    return context_results

@st.cache_data(ttl=3600)
def summarize_context(context_chunks, query):
    if not context_chunks:
        return "This clip provides context related to the query."
        
    context_text = " ".join([chunk.get('chunk', '') for chunk in context_chunks])
    if not context_text.strip():
        return "This clip provides context related to the query."
        
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are a bot that helps provide short summaries of how the given podcast transcript clip is relevant to the query."},
                {"role": "user", "content": f"""Summarize this podcast clip: {context_text} and how the conversation is relevant to the {query}, keep your response to 60 words or less and start with 'This clip is relevant to {query} because...'"""}
            ]
        )
        return response.choices[0].message.content
    except Exception as e:
        print(f"Error summarizing context: {e}")
        return "This clip is relevant to the query because it contains related discussion points."

async def process_search_results(search_results, query):
    if not search_results:
        return []
        
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor() as executor:
        tasks = [
            loop.run_in_executor(executor, summarize_context, result['context'], query)
            for result in search_results
        ]
        return await asyncio.gather(*tasks)
    
# Streamlit app
st.title("DOAC Semantic Search")

query = st.text_input("Enter your query:")

# Add date range filters
st.subheader("Filters")

# Create two columns for the date range
date_col1, date_col2 = st.columns(2)

# Default date range (last 2 years to today)
default_start_date = date.today() - timedelta(days=730)
default_end_date = date.today()

with date_col1:
    start_date = st.date_input("From date", default_start_date)
with date_col2:
    end_date = st.date_input("To date", default_end_date)

# Ensure start_date is before end_date
if start_date > end_date:
    st.error("End date must be after start date")
    start_date = end_date - timedelta(days=1)

# Add new filtering controls with better layout
st.subheader("Result Filtering")

# Create a container with custom styling for the filtering options
with st.container():
    # Use columns with better proportions
    filter_col1, filter_col2 = st.columns([3, 2])
    
    with filter_col1:
        # Add slider for max results per video (1-10, default: 2)
        max_results_per_video = st.slider(
            "Maximum clips per episode", 
            min_value=1, 
            max_value=10, 
            value=2,
            help="Control how many clips from the same episode can appear in results"
        )
    
    with filter_col2:
        st.write("")  # Add some spacing
        st.write("")  # Add some spacing
        # Add checkbox to disable per-episode limits with better alignment
        disable_limits = st.checkbox(
            "Disable per-episode limits", 
            value=False,
            help="Show all matching clips regardless of which episode they're from"
        )
        if disable_limits:
            max_results_per_video = 100  # Set to a high number effectively disabling the limit

if st.button("Search"):
    if query:
        with st.spinner("Searching for relevant clips..."):
            try:
                # Set fixed number of results to display
                max_results_to_display = 10
                
                # Set minimum desired results
                min_desired_results = 5
                
                # Perform the search with date filter
                search_results = semantic_search_with_context(query, top_k=30, start_date=start_date, end_date=end_date)
                
                # Display a message if no results were found
                if not search_results:
                    st.warning(f"No results found for '{query}' between {start_date.strftime('%B %d, %Y')} and {end_date.strftime('%B %d, %Y')}. Try expanding your date range or modifying your search query.")
                else:
                    # Count in-range results before filtering
                    in_range_results = [r for r in search_results if r.get('in_date_range', True)]
                    
                    # Set up to display results
                    display_results = in_range_results[:max_results_to_display]
                    
                    with st.spinner("Generating summaries..."):
                        summaries = asyncio.run(process_search_results(display_results, query))
                    
                    # Pre-filter results to count how many will actually be displayed
                    filtered_count = 0
                    displayed_videos = {}
                    displayed_start_times = {}
                    
                    # Simulate display filtering to get accurate count
                    for result in display_results:
                        episode_id = result['episode_id']
                        start_time = result['start_time']
                        
                        # Skip similar timestamps
                        is_close = False
                        displayed_start_times.setdefault(episode_id, [])
                        for displayed_time in displayed_start_times[episode_id]:
                            if abs(float(start_time) - float(displayed_time)) <= 120:
                                is_close = True
                                break
                        
                        if is_close:
                            continue
                        
                        # Apply per-video limit based on user settings
                        displayed_videos.setdefault(episode_id, 0)
                        guest_name_in_query = extract_guest_name(query) is not None
                        
                        # Only apply per-episode limits if not disabled and not a guest name search
                        if (not disable_limits and 
                            not guest_name_in_query and 
                            displayed_videos[episode_id] >= max_results_per_video):
                            continue
                        
                        displayed_videos[episode_id] += 1
                        displayed_start_times[episode_id].append(start_time)
                        filtered_count += 1
                    
                    # Now show accurate count
                    with st.container():
                        st.markdown(f"### Search Results")
                        st.markdown(f"Showing **{filtered_count}** of {len(in_range_results)} results within your date range.")
                        
                        # Display filtering message based on user settings
                        if disable_limits:
                            st.markdown("*Showing all matching results without diversity filtering.*")
                        else:
                            st.markdown(f"*Displaying most relevant results with max {max_results_per_video} clips per episode for diversity.*")
                        
                        # Show date range reminder
                        st.markdown(f"**Date Range:** {start_date.strftime('%B %d, %Y')} to {end_date.strftime('%B %d, %Y')}")
                    
                    # Reset for actual display
                    displayed_videos = {}
                    displayed_results = 0
                    displayed_start_times = {}
                    
                    # Display results 
                    for i, (result, summary) in enumerate(zip(display_results, summaries)):
                        episode_id = result['episode_id']
                        start_time = result['start_time']
                        
                        # Skip very similar timestamps
                        is_close = False
                        displayed_start_times.setdefault(episode_id, [])
                        for displayed_time in displayed_start_times[episode_id]:
                            if abs(float(start_time) - float(displayed_time)) <= 120:
                                is_close = True
                                break
                        
                        if is_close:
                            continue
                        
                        # Apply per-video limit based on user settings
                        displayed_videos.setdefault(episode_id, 0)
                        
                        # Apply per-video limit according to user settings
                        guest_name_in_query = extract_guest_name(query) is not None
                        if (not disable_limits and 
                            not guest_name_in_query and 
                            displayed_videos[episode_id] >= max_results_per_video):
                            continue
                        
                        displayed_videos[episode_id] += 1
                        displayed_results += 1
                        displayed_start_times[episode_id].append(start_time)
                        
                        if displayed_results > 1:
                            st.markdown("---")
                            st.markdown("<br>", unsafe_allow_html=True)
                        
                        # Add visual indicator if result is outside date range
                        if not result.get('in_date_range', True):
                            st.info("**Note:** This result is outside your selected date range but may still be relevant.")
                        
                        # --- FIX: Clean up episode name before displaying ---
                        cleaned_episode_name = result['episode_name'].replace('_', ' ')
                        st.subheader(f"🎙️ {cleaned_episode_name}")
                        
                        st.markdown(f"**👤 Guest:** {result['guest_name']}")
                        
                        # Convert the release_date string to a datetime object and format it
                        try:
                            if result['release_date']:
                                # Handle different date formats ('YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS')
                                cleaned_date_str = result['release_date'].split(' ')[0]
                                release_date = datetime.datetime.strptime(cleaned_date_str, '%Y-%m-%d').strftime('%B %d, %Y')
                                st.markdown(f"**📅 Release Date:** {release_date}")
                        except Exception as e:
                            # If date parsing fails, display raw or skip
                            if result['release_date']:
                                st.markdown(f"**📅 Release Date:** {result['release_date']}")
                            
                        st.markdown(f"**💡 Why is this Relevant:** {summary}")
                        
                        formatted_time = str(datetime.timedelta(seconds=int(float(result['start_time']))))
                        st.markdown(f"**🕒 Time:** {formatted_time}")
                        
                        # Handle intensity score which might be None or 0
                        intensity_score = float(result['intensity_score'] or 0)
                        formatted_intensity_score = f"{intensity_score * 100:.2f}%"
                        st.markdown(f"**🔥 Intensity Score:** {formatted_intensity_score}")
                        st.progress(int(min(intensity_score * 100, 100)))  # Ensure progress doesn't exceed 100
                        
                        formatted_relevance_score = f"{float(result['relevance_score']) * 100:.2f}%"
                        st.markdown(f"**🎯 Relevance Score:** {formatted_relevance_score}")

                        youtube_link = f"https://www.youtube.com/embed/{result['episode_id']}?start={int(float(result['start_time']))}"
                        st.markdown(f'<iframe width="560" height="315" src="{youtube_link}" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>', unsafe_allow_html=True)

                        with st.expander("📝 Transcript"):
                            for chunk in result['context']:
                                lines = chunk.get('chunk_with_speaker', '').split('\n')
                                for line in lines:
                                    if ':' in line:
                                        parts = line.split(':', 1)
                                        if len(parts) == 2:
                                            speaker, text = parts
                                            st.markdown(f"**{speaker}:**\t{text.strip()}", unsafe_allow_html=True)
                                        else:
                                            st.markdown(line, unsafe_allow_html=True)
                                    else:
                                        st.markdown(line, unsafe_allow_html=True)
                        
                        with st.expander("📊 Metadata"):
                            tab1, tab2 = st.tabs(["Clip Metadata", "Episode Metadata"])
                            
                            with tab1:
                                retention = float(result['relativeRetentionPerformance'] or 0)
                                st.markdown(f"**Relative Retention Performance:** {retention:.2f}")
                                st.info('How well this moment retains viewers compared to other videos of similar length on YouTube.', icon="ℹ️")
                                
                                watch_ratio = float(result['audienceWatchRatio'] or 0)
                                st.markdown(f"**Audience Watch Ratio:** {watch_ratio:.2f}")
                                st.info('The absolute ratio of viewers watching the video at the given point.', icon="ℹ️")
                                
                                st.markdown(f"**Intensity Score:** {formatted_intensity_score}")
                                st.info('How often this moment is replayed, where 100% represents the most replayed moment in the entire video.', icon="ℹ️")
                            
                            with tab2:
                                views = int(float(result['views'] or 0))
                                st.markdown(f"**Views:** {views:,}")
                                
                                minutes_watched = int(float(result['estimatedMinutesWatched'] or 0))
                                st.markdown(f"**Estimated Minutes Watched:** {minutes_watched:,} minutes")
                                
                                view_duration = int(float(result['averageViewDuration'] or 0))
                                st.markdown(f"**Average View Duration:** {view_duration:,} seconds")
                                
                                view_pct = float(result['averageViewPercentage'] or 0)
                                st.markdown(f"**Average View Percentage:** {view_pct:.2f}%")
                                
                                subs_gained = int(float(result['subscribersGained'] or 0))
                                st.markdown(f"**Subscribers Gained:** {subs_gained:,}")
                                
                                subs_lost = int(float(result['subscribersLost'] or 0))
                                st.markdown(f"**Subscribers Lost:** {subs_lost:,}")
                                
                                likes = int(float(result['likes'] or 0))
                                st.markdown(f"**Likes:** {likes:,}")
                                
                                dislikes = int(float(result['dislikes'] or 0))
                                st.markdown(f"**Dislikes:** {dislikes:,}")
                                
                                comments = int(float(result['comments'] or 0))
                                st.markdown(f"**Comments:** {comments:,}")
                                
                                shares = int(float(result['shares'] or 0))
                                st.markdown(f"**Shares:** {shares:,}")
                                
                                revenue = float(result['estimatedRevenue'] or 0)
                                st.markdown(f"**Estimated Revenue:** ${revenue:,.2f}")
            except Exception as e:
                st.error(f"An error occurred during search: {str(e)}")
                st.error("Please try again with a different query or contact support.")
    else:
        st.warning("Please enter a query.")

# Add custom CSS for better styling
st.markdown("""
<style>
    /* Progress bar styling */
    .stProgress > div > div > div > div {
        background-color: #4CAF50;
    }
    
    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre-wrap;
        background-color: #F0F2F6;
        border-radius: 4px;
        color: #4F8BF9;
        font-weight: bold;
    }
    .stTabs [aria-selected="true"] {
        background-color: #4F8BF9;
        color: white;
    }
    
    /* Filter controls styling */
    [data-testid="stSubheader"]:has(> div:contains("Result Filtering")) {
        margin-top: 1.5rem;
        margin-bottom: 0.5rem;
    }
    
    /* Checkbox alignment */
    .stCheckbox {
        padding-top: 0.5rem;
    }
    
    /* Add spacing between filters and search button */
    [data-testid="stButton"] {
        margin-top: 1.5rem;
    }
    
    /* Better container spacing */
    .stContainer {
        padding: 1rem 0;
    }
</style>
""", unsafe_allow_html=True) 