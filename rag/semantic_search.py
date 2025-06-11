import pinecone
from openai import OpenAI, ChatCompletion
import datetime
import streamlit as st
import spacy
import os

# Initialize Pinecone and OpenAI clients
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if not PINECONE_API_KEY:
    raise ValueError("No Pinecone API key found in environment variables. Please set the PINECONE_API_KEY.")
if not OPENAI_API_KEY:
    raise ValueError("No OpenAI API key found in environment variables. Please set the OPENAI_API_KEY.")

pc = pinecone.Pinecone(api_key=PINECONE_API_KEY)
index = pc.Index("youtube-transcripts-embeddings-no-speaker5")
guest_index = pc.Index("youtube-transcripts-embeddings-speaker10")
client = OpenAI(api_key=OPENAI_API_KEY)

# Load SpaCy model
nlp = spacy.load("en_core_web_trf")  # Use transformer-based model for better accuracy

def enhanced_query(query):
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "system", "content":"""You are a query enhancer for a podcast transcript search system. 
Expand the given query to include related terms, synonyms, and contextual information 
relevant to podcasts. Focus on creating a comprehensive query that captures the user's intent 
and related concepts. Return only the enhanced query without any additional text but try to keep it as short as possible."""},
                  {"role": "user", "content": f"Enhance this query for better search results: {query}"}]
    )
    enhanced_query_text = response.choices[0].message.content.strip()
    print(f"Enhanced Query: {enhanced_query_text}")  # Print the enhanced query
    return enhanced_query_text

def extract_guest_name(query):
    doc = nlp(query)
    for ent in doc.ents:
        if ent.label_ == "PERSON":
            return ent.text
    print("No name specified")
    return None

def semantic_search_with_context(query, top_k=5, context_window=5):
    enhanced_query_text = enhanced_query(query)
    query_embedding = client.embeddings.create(input=enhanced_query_text, model="text-embedding-ada-002").data[0].embedding
    
    # Extract guest name from the query if mentioned
    guest_name = extract_guest_name(query)
    
    # Choose the appropriate index
    if guest_name:
        results = guest_index.query(vector=query_embedding, top_k=top_k, include_metadata=True)
        print("Querying speaker db")
    else:
        results = index.query(vector=query_embedding, top_k=top_k, include_metadata=True)
        print("Querying no speaker db")
    
    context_results = []
    for match in results['matches']:
        episode_id = match['metadata']['episode_id']
        start_time = match['metadata']['start_time']
        
        if start_time < 90:  # Exclude transcripts with start time before 90 seconds
            continue
        
        # Adjust the filter to get chunks within 2-4 minutes of the start_time
        nearby_chunks = (guest_index if guest_name else index).query(
            vector=query_embedding,
            filter={
                "episode_id": {"$eq": episode_id},
                "start_time": {"$gte": start_time, "$lte": start_time + 60}  # 2 minutes before to 4 minutes after
            },
            top_k=100,  # Increase top_k to ensure we get enough chunks
            include_metadata=True
        )
        
        # Combine and sort chunks by start_time and chunk_index
        combined_chunks = sorted(
            nearby_chunks['matches'], 
            key=lambda x: (x['metadata']['start_time'], x['metadata'].get('chunk_index', 0))
        )
        
        # Aggregate chunks with the same start_time
        aggregated_chunks = []
        current_chunk = None
        for chunk in combined_chunks:
            if current_chunk and chunk['metadata']['start_time'] == current_chunk['start_time']:
                current_chunk['chunk'] += "\n\n" + chunk['metadata']['chunk']
                current_chunk['chunk_with_speaker'] += "\n\n" + chunk['metadata']['chunk_with_speaker']
            else:
                if current_chunk:
                    aggregated_chunks.append(current_chunk)
                current_chunk = {
                    "speaker": chunk['metadata']['speaker'],
                    "chunk": chunk['metadata']['chunk'],
                    "chunk_with_speaker": chunk['metadata']['chunk_with_speaker'],
                    "start_time": chunk['metadata']['start_time']
                }
        if current_chunk:
            aggregated_chunks.append(current_chunk)
        
        context_results.append({
            "question": match['metadata']['chunk'],
            'chunk_with_speaker':match['metadata']['chunk_with_speaker'],
            "context": aggregated_chunks,
            "speaker": match['metadata']['speaker'],
            "guest_name": match['metadata']['guest_name'],
            "episode_name": match['metadata']['episode_name'],
            "start_time": match['metadata']['start_time'],
            "intensity_score": match['metadata']['intensityScoreNormalized'],
            "episode_id": episode_id,
            "relevance_score": match['score'],
            "release_date": match['metadata']['release_date'],
            'views': match['metadata']['views'],
            'estimatedMinutesWatched': match['metadata']['estimatedMinutesWatched'],
            'averageViewDuration': match['metadata']['averageViewDuration'],
            'averageViewPercentage': match['metadata']['averageViewPercentage'],
            'subscribersGained': match['metadata']['subscribersGained'],
            'subscribersLost': match['metadata']['subscribersLost'],
            'likes': match['metadata']['likes'],
            'dislikes': match['metadata']['dislikes'],
            'comments': match['metadata']['comments'],
            'shares': match['metadata']['shares'],
            'estimatedRevenue': match['metadata']['estimatedRevenue'],
            'intensityScoreNormalized': match['metadata']['intensityScoreNormalized'] if match['metadata']['intensityScoreNormalized'] is not None else 0,
            'relativeRetentionPerformance': match['metadata']['relativeRetentionPerformance'] if match['metadata']['relativeRetentionPerformance'] is not None else 0,
            'audienceWatchRatio': match['metadata']['audienceWatchRatio'] if match['metadata']['audienceWatchRatio'] is not None else 0
        })
    
    return context_results

def summarize_context(context_chunks, query):
    context_text = " ".join([chunk['chunk'] for chunk in context_chunks])
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "system", "content": "You are a bot that helps provide short summarys of the how the given podcast transcript clip is relevant to the query."},
                  {"role": "user", "content": f"""Summarize this podcast clip: {context_text} and how the conversation is relevant to the {query}, keep your response to 60 words or less and start with 'This clips is relavant to {query} because...'"""}]
    )
    return response.choices[0].message.content

# Streamlit app
st.title("DOAC Semantic Search")
query = st.text_input("Enter your query:")

if st.button("Search"):
    if query:
        search_results = semantic_search_with_context(query)
        displayed_videos = set()
        
        for i, result in enumerate(search_results):
            if not extract_guest_name(query) and result['episode_id'] in displayed_videos:
                continue
            
            displayed_videos.add(result['episode_id'])
            
            # Add a horizontal line and spacing between results
            if i > 0:
                st.markdown("---")
                st.markdown("<br>", unsafe_allow_html=True)
            
            st.subheader(f"🎙️ {result['episode_name']}")
            st.markdown(f"**👤 Guest:** {result['guest_name']}")
            
            summary = summarize_context(result['context'], query)
            st.markdown(f"**💡 Why is this Relevant:** {summary}")
            
            formatted_time = str(datetime.timedelta(seconds=int(result['start_time'])))
            st.markdown(f"**🕒 Time:** {formatted_time}")
            
            formatted_intensity_score = f"{result['intensity_score'] * 100:.2f}%"
            st.markdown(f"**🔥 Intensity Score:** {formatted_intensity_score}")
            st.progress(int(result['intensity_score'] * 100))
            
            formatted_relevance_score = f"{result['relevance_score'] * 100:.2f}%"
            st.markdown(f"**🎯 Relevance Score:** {formatted_relevance_score}")

            youtube_link = f"https://www.youtube.com/embed/{result['episode_id']}?start={int(result['start_time'])}"
            st.markdown(f'<iframe width="560" height="315" src="{youtube_link}" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>', unsafe_allow_html=True)

            with st.expander("📝 Transcript"):
                for chunk in result['context']:
                    lines = chunk['chunk_with_speaker'].split('\n')
                    for line in lines:
                        if ':' in line:
                            speaker, text = line.split(':', 1)
                            st.markdown(f"**{speaker}:**\t{text.strip()}", unsafe_allow_html=True)
                        else:
                            st.markdown(line, unsafe_allow_html=True)
            
            # Use an expander for metadata tabs
            with st.expander("📊 Metadata"):
                tab1, tab2 = st.tabs(["Clip Metadata", "Episode Metadata"])
                
                with tab1:
                    st.markdown(f"**Relative Retention Performance:** {result['relativeRetentionPerformance']:.2f}")
                    st.info('How well this moment retains viewers compared to other videos of similar length on YouTube.', icon="ℹ️")
                    st.markdown(f"**Audience Watch Ratio:** {result['audienceWatchRatio']:.2f}")
                    st.info('The absolute ratio of viewers watching the video at the given point.', icon="ℹ️")
                    st.markdown(f"**Intensity Score:** {formatted_intensity_score}")
                    st.info('How often this moment is replayed, where 100% represents the most replayed moment in the entire video.', icon="ℹ️")
                
                with tab2:
                    st.markdown(f"**Views:** {result['views']:,}")
                    st.markdown(f"**Estimated Minutes Watched:** {result['estimatedMinutesWatched']:,} minutes")
                    st.markdown(f"**Average View Duration:** {result['averageViewDuration']:,} seconds")
                    st.markdown(f"**Average View Percentage:** {result['averageViewPercentage']:.2f}%")
                    st.markdown(f"**Subscribers Gained:** {result['subscribersGained']:,}")
                    st.markdown(f"**Subscribers Lost:** {result['subscribersLost']:,}")
                    st.markdown(f"**Likes:** {result['likes']:,}")
                    st.markdown(f"**Dislikes:** {result['dislikes']:,}")
                    st.markdown(f"**Comments:** {result['comments']:,}")
                    st.markdown(f"**Shares:** {result['shares']:,}")
                    st.markdown(f"**Estimated Revenue:** ${result['estimatedRevenue']:,.2f}")
    else:
        st.warning("Please enter a query.")

# Add custom CSS for better styling
st.markdown("""
<style>
    .stProgress > div > div > div > div {
        background-color: #4CAF50;
    }
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
</style>
""", unsafe_allow_html=True)