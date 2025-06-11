from googleapiclient.discovery import build
from google.cloud import bigquery
import os
import logging
import pandas as pd
import json

# Initialize logging
logging.basicConfig(level=logging.INFO)

# Path to your service account JSON key file
SERVICE_ACCOUNT_FILE = 'flightstudio-d8c6c3039d4c.json'

# Set the path to your service account key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_FILE

# Initialize BigQuery client
def get_bq_client():
    bq_client = bigquery.Client()
    return bq_client

# Initialize BigQuery client
bq_client = get_bq_client()

# Function to read CSV and prepare data
def read_csv_and_prepare_data(csv_file_path):
    df = pd.read_csv(csv_file_path)
    df = df.rename(columns={
        'Content': 'episode_id',
        'Video title': 'video_title',
        'Video publish time': 'video_publish_time',
        'Views': 'views',
        'Watch time (hours)': 'watch_time_hours',
        'Subscribers': 'subscribers',
        'Estimated revenue (GBP)': 'estimated_revenue_gbp',
        'Impressions': 'impressions',
        'Impressions click-through rate (%)': 'impressions_click_through_rate'
    })
    
    # Replace NaN values with None (to avoid JSON serialization issues)
    df = df.where(pd.notnull(df), None)
    
    # Ensure numeric columns are properly typed and NaNs are replaced with 0
    df['views'] = df['views'].astype('float').fillna(0)
    df['watch_time_hours'] = df['watch_time_hours'].astype('float').fillna(0)
    df['subscribers'] = df['subscribers'].astype('float').fillna(0)
    df['estimated_revenue_gbp'] = df['estimated_revenue_gbp'].astype('float').fillna(0)
    df['impressions'] = df['impressions'].astype('Int64').fillna(0)
    df['impressions_click_through_rate'] = df['impressions_click_through_rate'].astype('float').fillna(0)
    
    return df

# Function to create a new BigQuery table
def create_new_table(table_id, df):
    schema = []
    for column in df.columns:
        field_type = 'STRING' if df[column].dtype == 'object' else 'FLOAT' if df[column].dtype in ['float64', 'float'] else 'INTEGER'
        schema.append(bigquery.SchemaField(column, field_type))
    
    table = bigquery.Table(table_id, schema=schema)
    table = bq_client.create_table(table)
    logging.info(f"Created table {table_id}")

# Function to upload data to BigQuery table
def upload_data_to_bq(table_id, data):
    # Convert data to JSON serializable format
    json_data = json.loads(json.dumps(data, default=str))
    
    errors = bq_client.insert_rows_json(table_id, json_data)
    if errors:
        logging.error(f"Encountered errors while inserting rows: {errors}")

# Main function
def main():
    csv_file_path = 'youtube_analytics_2024_08_29.csv'  # Update with your actual CSV file path
    table_id = f"flightstudio.youtube_transcript_data.{os.path.splitext(os.path.basename(csv_file_path))[0].replace(' ', '_').lower()}"
    
    df = read_csv_and_prepare_data(csv_file_path)
    create_new_table(table_id, df)
    data = df.to_dict(orient='records')
    upload_data_to_bq(table_id, data)

if __name__ == "__main__":
    main()
