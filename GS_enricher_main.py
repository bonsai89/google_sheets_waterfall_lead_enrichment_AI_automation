import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import requests
import time
from config import GOOGLE_SHEETS, BRIGHT_DATA
from snapshot_monitor import process_snapshots

def read_google_sheet():
    """
    Reads links from a specified Google Sheet and worksheet using configuration settings.
    
    Returns:
        list: List of links from the specified column
    """
    # Define the scope
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']

    # Use credentials file path from config
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        GOOGLE_SHEETS['credentials_file'], 
        scope
    )

    # Authorize the client
    client = gspread.authorize(credentials)

    # Open the Google Sheet using config
    sheet = client.open(GOOGLE_SHEETS['sheet_name'])

    # Select the worksheet using config
    worksheet = sheet.worksheet(GOOGLE_SHEETS['worksheet_name'])

    # Get all values from the worksheet
    all_values = worksheet.get_all_values()
    
    # Get header row
    headers = all_values[0]
    
    # Find the index of the column with links
    try:
        column_index = headers.index(GOOGLE_SHEETS['column_with_links']) + 1  # +1 because gspread uses 1-based indexing
    except ValueError:
        raise ValueError(f"Column '{GOOGLE_SHEETS['column_with_links']}' not found in the sheet. Available columns: {headers}")

    # Get all values from the specified column
    links = worksheet.col_values(column_index)
    
    # Remove header if present
    if links:
        links = links[1:]
    
    return links

def chunk_list(lst, chunk_size):
    """Split a list into chunks of specified size."""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

def process_links_with_bright_data(links):
    """
    Process links using Bright Data API in chunks.
    
    Args:
        links (list): List of URLs to process
    """
    # Remove empty strings
    clean_links = [link for link in links if link.strip()]
    
    # Print statistics
    print(f"Total links found: {len(links)}")
    print(f"Clean links (non-empty): {len(clean_links)}")
    
    # Prepare API headers and parameters
    headers = {
        "Authorization": f"Bearer {BRIGHT_DATA['api_key']}",
        "Content-Type": "application/json",
    }
    
    params = {
        "dataset_id": BRIGHT_DATA['dataset_id'],
        "include_errors": "true",
    }
    
    # Process links in chunks
    for i, chunk in enumerate(chunk_list(clean_links, BRIGHT_DATA['chunk_size']), start=1):
        payload = [{"url": url} for url in chunk]
        print(f"🚀 Sending chunk {i} with {len(chunk)} URLs...")
        
        # Retry logic with exponential backoff
        max_retries = 3
        base_delay = 5  # Base delay in seconds
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                response = requests.post(
                    BRIGHT_DATA['api_url'],
                    headers=headers,
                    params=params,
                    json=payload
                )
                
                if response.status_code == 200:
                    print(f"✅ Chunk {i} submitted successfully")
                    break  # Success, exit retry loop
                else:
                    print(f"❌ Failed to submit chunk {i} - Status Code: {response.status_code}")
                    print(f"Response: {response.text}")
                    
                    if retry_count < max_retries - 1:  # Don't wait on last retry
                        delay = base_delay * (2 ** retry_count)  # Exponential backoff
                        print(f"⏳ Retrying in {delay} seconds... (Attempt {retry_count + 1}/{max_retries})")
                        time.sleep(delay)
                    
            except Exception as e:
                print(f"❌ Error processing chunk {i}: {str(e)}")
                if retry_count < max_retries - 1:  # Don't wait on last retry
                    delay = base_delay * (2 ** retry_count)  # Exponential backoff
                    print(f"⏳ Retrying in {delay} seconds... (Attempt {retry_count + 1}/{max_retries})")
                    time.sleep(delay)
            
            retry_count += 1
        
        if retry_count == max_retries:
            print(f"⚠️ Failed to process chunk {i} after {max_retries} attempts. Moving to next chunk...")
            
        # Add delay between chunks to avoid rate limits
        time.sleep(5)

def main():
    try:
        # Read links from Google Sheet
        links = read_google_sheet()
        print("Links found in the sheet:")
        for link in links:
            print(link)
        
        # Process links with Bright Data
        process_links_with_bright_data(links)
        
        # Monitor and process snapshots
        print("\nStarting snapshot monitoring...")
        process_snapshots()
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()
