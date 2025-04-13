import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import requests
import time
from config import GOOGLE_SHEETS, BRIGHT_DATA
from snapshot_monitor import process_profile_snapshots, process_company_snapshots, update_lead_scores

def get_google_sheet_client():
    """Initialize and return Google Sheets client."""
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        GOOGLE_SHEETS['credentials_file'], 
        scope
    )
    return gspread.authorize(credentials)

def extract_company_links(worksheet):
    """
    Extract company LinkedIn URLs from the 'current_company' column.
    
    Args:
        worksheet: Google Sheet worksheet object
    
    Returns:
        list: List of company LinkedIn URLs
    """
    # Get all values from the worksheet
    all_values = worksheet.get_all_values()
    headers = all_values[0]
    
    # Find the index of the current_company column
    try:
        column_index = headers.index('current_company')
    except ValueError:
        raise ValueError("Column 'current_company' not found in the sheet")
    
    # Get all values from the current_company column (excluding header)
    company_data = [row[column_index] for row in all_values[1:] if row[column_index]]
    
    company_links = set()  # Use set to automatically handle duplicates
    for entry in company_data:
        # Parse the company data
        parts = entry.split('|')
        for part in parts:
            part = part.strip()
            if part.startswith('link:'):
                # Extract URL from the link field
                url = part.replace('link:', '').strip()
                # Remove any tracking parameters and normalize URL
                url = url.split('?')[0].rstrip('/')
                company_links.add(url)
            elif part.startswith('company_id:'):
                # Construct URL from company ID
                company_id = part.replace('company_id:', '').strip()
                url = f"https://www.linkedin.com/company/{company_id}"
                company_links.add(url)
    
    return list(company_links)


def read_google_sheet():
    """Read data from Google Sheet and extract company links."""
    try:
        # Initialize Google Sheets client
        client = get_google_sheet_client()
        sheet = client.open(GOOGLE_SHEETS['sheet_name'])
        worksheet = sheet.worksheet(GOOGLE_SHEETS['worksheet_name'])
        
        # Get all values
        data = worksheet.get_all_values()
        
        # Check if sheet is empty or has no headers
        if not data or len(data) < 2:  # Less than 2 rows means only headers or empty
            print("ℹ️ Sheet is empty or has no data rows yet")
            return []
            
        # Get headers
        headers = data[0]
        
        # Check if current_company column exists
        if 'current_company' not in headers:
            print("ℹ️ current_company column not found in sheet yet")
            return []
            
        # Get column index for current_company
        company_col = headers.index('current_company')
        
        # Extract company links from the current_company column
        company_links = set()  # Use set to avoid duplicates
        for row in data[1:]:  # Skip header row
            if len(row) > company_col:  # Check if row has enough columns
                company_value = row[company_col].strip()
                if company_value:  # Only process non-empty values
                    # Extract URL from the company data
                    parts = company_value.split('|')
                    for part in parts:
                        part = part.strip()
                        if part.startswith('link:'):
                            url = part.replace('link:', '').strip()
                            # Normalize URL by removing tracking parameters and trailing slashes
                            url = url.split('?')[0].rstrip('/')
                            company_links.add(url)
                        elif part.startswith('company_id:'):
                            company_id = part.replace('company_id:', '').strip()
                            url = f"https://www.linkedin.com/company/{company_id}"
                            company_links.add(url)
        
        # Convert set to sorted list for consistent output
        company_links = sorted(list(company_links))
        print(f"📊 Found {len(company_links)} unique company links in sheet")
        return company_links
        
    except Exception as e:
        print(f"❌ Error reading Google Sheet: {str(e)}")
        return []

def chunk_list(lst, chunk_size):
    """Split a list into chunks of specified size."""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

def process_links_with_bright_data(links, is_company=False):
    """
    Process links using Bright Data API in chunks.
    
    Args:
        links (list): List of URLs to process
        is_company (bool): Whether processing company links (True) or profile links (False)
    """
    # Remove empty strings and duplicates
    clean_links = list(set([link for link in links if link.strip()]))
    
    # Print statistics
    print(f"Total links found: {len(links)}")
    print(f"Clean unique links: {len(clean_links)}")
    
    # Prepare API headers and parameters
    headers = {
        "Authorization": f"Bearer {BRIGHT_DATA['api_key']}",
        "Content-Type": "application/json",
    }
    
    # Use appropriate dataset ID based on type
    dataset_id = BRIGHT_DATA['company_dataset_id'] if is_company else BRIGHT_DATA['profile_dataset_id']
    if not dataset_id:
        raise ValueError(f"Dataset ID not found in config for {'company' if is_company else 'profile'} scraping")
    
    params = {
        "dataset_id": dataset_id,
        "include_errors": "true",
    }
    
    print(f"Using dataset ID: {dataset_id}")
    
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

def read_profile_links():
    """
    Reads profile links from the Google Sheet.
    
    Returns:
        list: List of LinkedIn profile URLs
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
    headers = all_values[0]
    
    # Find the index of the column with profile links
    try:
        column_index = headers.index(GOOGLE_SHEETS['column_with_links'])
    except ValueError:
        raise ValueError(f"Column '{GOOGLE_SHEETS['column_with_links']}' not found in the sheet")
    
    # Get all values from the specified column (excluding header)
    profile_links = [row[column_index] for row in all_values[1:] if row[column_index]]
    
    return profile_links

def main():
    try:
        # First process profile links
        print("📊 Reading profile links from Google Sheet...")
        profile_links = read_profile_links()
        
        print("\nProfile links found:")
        for link in profile_links:
            print(f"🔗 {link}")
        
        print("\n🔄 Processing profile links with Bright Data...")
        process_links_with_bright_data(profile_links, is_company=False)
        
        # Wait for profile snapshots to be processed
        print("\n👀 Starting profile snapshot monitoring...")
        process_profile_snapshots()
        
        # Only after profile processing is complete, process company links
        print("\n📊 Reading company links from Google Sheet...")
        company_links = read_google_sheet()
        
        if company_links:
            print("\nCompany links found:")
            for link in company_links:
                print(f"🔗 {link}")
            
            print("\n🔄 Processing company links with Bright Data...")
            process_links_with_bright_data(company_links, is_company=True)
            
            # Process company snapshots
            print("\n👀 Starting company snapshot monitoring...")
            process_company_snapshots()
        else:
            print("\nℹ️ No company links found or current_company column not available yet")
        
        # After all processing is complete, update lead scores
        print("\n📊 Starting lead scoring...")
        update_lead_scores()
    except Exception as e:
        print(f"❌ An error occurred: {str(e)}")

if __name__ == "__main__":
    main()
