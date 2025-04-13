import requests
import os
import json
import time
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from config import GOOGLE_SHEETS, BRIGHT_DATA, OPENAI, LEAD_SCORING
import openai

# Constants
SNAPSHOTS_LIST_URL = "https://api.brightdata.com/datasets/v3/snapshots"
SNAPSHOT_FETCH_URL = "https://api.brightdata.com/datasets/v3/snapshot/"
HEADERS = {"Authorization": f"Bearer {BRIGHT_DATA['api_key']}"}
SNAPSHOT_PARAMS = {"format": "json"}

# Separate directories for profile and company snapshots
PROFILE_SAVE_DIR = "profile_snapshots"
COMPANY_SAVE_DIR = "company_snapshots"
PROFILE_PROCESSED_FILE = "processed_profile_snapshots.json"
COMPANY_PROCESSED_FILE = "processed_company_snapshots.json"
PROFILE_UPDATED_FILE = "updated_profile_snapshots.json"
COMPANY_UPDATED_FILE = "updated_company_snapshots.json"

# Google Sheets rate limiting
SHEETS_UPDATE_DELAY = 1.1  # Delay between updates in seconds (slightly more than 1 second)
BATCH_SIZE = 50  # Number of cells to update in a single batch

def get_google_sheet_client():
    """Initialize and return Google Sheets client."""
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        GOOGLE_SHEETS['credentials_file'], 
        scope
    )
    return gspread.authorize(credentials)

def load_updated_snapshots(is_company=False):
    """Load the list of snapshots already updated in Google Sheet."""
    updated_file = COMPANY_UPDATED_FILE if is_company else PROFILE_UPDATED_FILE
    if os.path.exists(updated_file):
        with open(updated_file, "r") as f:
            return set(json.load(f))
    return set()

def save_updated_snapshots(updated_snapshots, is_company=False):
    """Save the list of snapshots updated in Google Sheet."""
    updated_file = COMPANY_UPDATED_FILE if is_company else PROFILE_UPDATED_FILE
    with open(updated_file, "w") as f:
        json.dump(list(updated_snapshots), f)

def format_value(value):
    """Convert nested JSON structures into human-readable strings."""
    if value is None:
        return ""
    elif isinstance(value, list):
        # Handle lists of dictionaries or simple values
        formatted_items = []
        for item in value:
            if isinstance(item, dict):
                # Convert dict to key: value format
                formatted_dict = []
                for k, v in item.items():
                    if isinstance(v, (list, dict)):
                        v = format_value(v)
                    formatted_dict.append(f"{k}: {v}")
                formatted_items.append(" | ".join(formatted_dict))
            else:
                formatted_items.append(str(item))
        return ", ".join(formatted_items)
    elif isinstance(value, dict):
        # Convert dict to key: value format
        formatted_items = []
        for k, v in value.items():
            if isinstance(v, (list, dict)):
                v = format_value(v)
            formatted_items.append(f"{k}: {v}")
        return " | ".join(formatted_items)
    else:
        return str(value)

def get_column_order():
    """Return the preferred order of columns and their groupings."""
    return {
        # Primary personal information
        'name': 1,
        'position': 2,
        'city': 3,
        'country_code': 4,
        'current_company_company_id': 5,
        'current_company': 6,
        'about': 7,
        'experience': 8,
        
        # Company related fields
        'company': 100,
        'company_size': 101,
        'company_industry': 102,
        'company_website': 103,
        'company_description': 104,
        'company_founded': 105,
        'company_specialties': 106,
        
        # Additional personal information
        'headline': 200,
        'summary': 201,
        'skills': 202,
        'education': 203,
        'languages': 204,
        'certifications': 205,
        'volunteer_experience': 206,
        'recommendations': 207,
        'connections': 208,
        
        # Contact information
        'email': 300,
        'phone': 301,
        'twitter': 302,
        'website': 303,
        
        # Other fields (will be added after the ordered ones)
        'other': 1000
    }

def update_similar_profiles(snapshot_data, snapshot_id):
    """Add similar profiles and people also viewed URLs to a separate worksheet."""
    try:
        client = get_google_sheet_client()
        sheet = client.open(GOOGLE_SHEETS['sheet_name'])
        
        # Try to get the 'Similar Leads' worksheet, create if it doesn't exist
        try:
            worksheet = sheet.worksheet('Similar Leads')
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sheet.add_worksheet(title='Similar Leads', rows=1000, cols=10)
        
        # Check if headers exist, add them if they don't
        current_headers = worksheet.row_values(1)
        if not current_headers or current_headers[0] != 'linkedin_person_url':
            worksheet.update([['linkedin_person_url', 'name', '', '', '', '', '', '', '', '']], 'A1:J1')
        
        # Get existing URLs from the sheet
        existing_urls = set(worksheet.col_values(1)[1:])  # Skip header row
        print(f"📊 Found {len(existing_urls)} existing URLs in sheet")
        
        # Collect all rows to update
        rows_to_update = []
        seen_urls = set()  # Track unique URLs
        
        # Process each profile in the snapshot
        for profile in snapshot_data:
            input_url = profile.get('input_url')
            if not input_url:
                continue
            
            print(f"🔄 Processing similar profiles for URL: {input_url}")
            
            # Process similar profiles
            similar_profiles = profile.get('similar_profiles', [])
            for similar in similar_profiles:
                url = similar.get('url')
                name = similar.get('name', '')
                if url and url not in seen_urls and url not in existing_urls:
                    seen_urls.add(url)
                    rows_to_update.append([url, name, '', '', '', '', '', '', '', ''])
            
            # Process people also viewed
            people_also_viewed = profile.get('people_also_viewed', [])
            for person in people_also_viewed:
                url = person.get('url')
                name = person.get('name', '')
                if url and url not in seen_urls and url not in existing_urls:
                    seen_urls.add(url)
                    rows_to_update.append([url, name, '', '', '', '', '', '', '', ''])
        
        # Update in batches
        if rows_to_update:
            print(f"📝 Found {len(rows_to_update)} new unique similar profiles to add...")
            # Get the next empty row
            next_row = len(worksheet.get_all_values()) + 1
            # Update all rows at once (fixing deprecation warning)
            worksheet.update(values=rows_to_update, range_name=f'A{next_row}:J{next_row + len(rows_to_update) - 1}')
            print(f"✅ Added {len(rows_to_update)} new unique similar profiles to sheet")
        else:
            print("ℹ️ No new similar profiles to add")
            
    except Exception as e:
        print(f"❌ Error adding similar profile URLs: {str(e)}")
        if "Quota exceeded" in str(e):
            print("⚠️ Google Sheets API quota exceeded. Waiting 60 seconds before retrying...")
            time.sleep(60)  # Wait 60 seconds before retrying
            return update_similar_profiles(snapshot_data, snapshot_id)  # Retry the update

def update_sheet1(profile_data, is_company=False):
    """Update Sheet1 with profile data."""
    try:
        # Initialize Google Sheets client
        client = get_google_sheet_client()
        spreadsheet = client.open(GOOGLE_SHEETS['sheet_name'])
        
        # Get the first worksheet (Sheet1)
        worksheet = spreadsheet.worksheet("Sheet1")
        
        # Define headers based on data type
        if is_company:
            headers = [
                "linkedin_company_url", "name", "country_code", "locations", "followers",
                "employees_in_linkedin", "about", "company_size", "organization_type",
                "industries", "website", "crunchbase_url", "founded", "company_id",
                "headquarters", "image", "logo", "slogan", "funding", "investors",
                "formatted_locations", "description", "website_simplified"
            ]
        else:
            headers = [
                "linkedin_person_url", "name", "headline", "location", "followers",
                "connections", "about", "experience", "education", "skills",
                "languages", "certifications", "projects", "volunteer", "awards",
                "publications", "courses", "test_scores", "organizations",
                "patents", "recommendations", "similar_profiles", "people_also_viewed"
            ]
        
        # Get existing data
        existing_data = worksheet.get_all_values()
        
        # If sheet is empty, add headers
        if not existing_data:
            worksheet.append_row(headers)
            existing_data = worksheet.get_all_values()
        
        # Get the URL column index
        url_col = headers.index("linkedin_company_url" if is_company else "linkedin_person_url")
        
        # Get existing URLs
        existing_urls = {row[url_col] for row in existing_data[1:] if len(row) > url_col}
        
        # Process new data
        new_rows = []
        for data in profile_data:
            url = data.get("url", "")
            if url and url not in existing_urls:
                row = []
                for header in headers:
                    value = data.get(header, "")
                    if isinstance(value, (list, dict)):
                        # Handle nested structures
                        if header in ["experience", "education", "skills", "languages", 
                                    "certifications", "projects", "volunteer", "awards",
                                    "publications", "courses", "test_scores", "organizations",
                                    "patents", "recommendations", "similar_profiles", 
                                    "people_also_viewed", "locations", "employees", "similar",
                                    "updates", "investors", "formatted_locations"]:
                            value = json.dumps(value, ensure_ascii=False)
                        else:
                            value = str(value)
                    row.append(value)
                new_rows.append(row)
                existing_urls.add(url)
        
        # Append new rows if any
        if new_rows:
            worksheet.append_rows(new_rows)
            print(f"✅ Added {len(new_rows)} new {'company' if is_company else 'profile'} records to Sheet1")
        else:
            print(f"ℹ️ No new {'company' if is_company else 'profile'} data to add to Sheet1")
            
    except Exception as e:
        print(f"❌ Error updating Sheet1: {str(e)}")

def get_company_column_order():
    """Return the preferred order of company columns and their groupings."""
    return {
        # Primary company information
        'name': 1,
        'country_code': 2,
        'locations': 3,
        'followers': 4,
        'employees_in_linkedin': 5,
        'about': 6,
        'company_size': 7,
        'organization_type': 8,
        'industries': 9,
        'website': 10,
        'crunchbase_url': 11,
        'founded': 12,
        'company_id': 13,
        'headquarters': 14,
        'slogan': 15,
        'description': 16,
        'website_simplified': 17,
        
        # Additional company information
        'employees': 100,
        'similar': 101,
        'updates': 102,
        'funding': 103,
        'investors': 104,
        'formatted_locations': 105,
        
        # Other fields (will be added after the ordered ones)
        'other': 1000
    }

def format_company_value(value):
    """Convert nested company JSON structures into human-readable strings."""
    if value is None:
        return ""
    elif isinstance(value, list):
        # Handle lists of dictionaries or simple values
        formatted_items = []
        for item in value:
            if isinstance(item, dict):
                # Convert dict to key: value format
                formatted_dict = []
                for k, v in item.items():
                    if isinstance(v, (list, dict)):
                        v = format_company_value(v)
                    formatted_dict.append(f"{k}: {v}")
                formatted_items.append(" | ".join(formatted_dict))
            else:
                formatted_items.append(str(item))
        return ", ".join(formatted_items)
    elif isinstance(value, dict):
        # Convert dict to key: value format
        formatted_items = []
        for k, v in value.items():
            if isinstance(v, (list, dict)):
                v = format_company_value(v)
            formatted_items.append(f"{k}: {v}")
        return " | ".join(formatted_items)
    else:
        return str(value)

def update_google_sheet(snapshot_data, snapshot_id, is_company=False):
    """Update Google Sheet with snapshot data."""
    try:
        client = get_google_sheet_client()
        sheet = client.open(GOOGLE_SHEETS['sheet_name'])
        worksheet = sheet.worksheet(GOOGLE_SHEETS['worksheet_name'])
        
        # Get all values to find the row with matching URL
        all_values = worksheet.get_all_values()
        headers = all_values[0]
        
        # Get column order based on type
        column_order = get_company_column_order() if is_company else get_column_order()
        
        # Process each profile/company in the snapshot
        for data in snapshot_data:
            if is_company:
                # For company data, we need to find the row by reconstructing the company URL
                company_url = data.get('input', {}).get('url')
                if not company_url:
                    continue
                    
                # Normalize the URL for comparison
                company_url = company_url.split('?')[0].rstrip('/')
                
                # Find the row by checking current_company column
                company_col = headers.index('current_company') if 'current_company' in headers else -1
                if company_col == -1:
                    print("❌ current_company column not found in sheet")
                    continue
                    
                # Get all values from current_company column
                company_values = worksheet.col_values(company_col + 1)  # +1 because gspread uses 1-based indexing
                
                # Find matching row
                row_index = -1
                for i, company_value in enumerate(company_values[1:], start=2):  # Skip header, start from row 2
                    if not company_value:
                        continue
                        
                    # Check each part of the company value
                    parts = company_value.split('|')
                    for part in parts:
                        part = part.strip()
                        if part.startswith('link:'):
                            url = part.replace('link:', '').strip()
                            url = url.split('?')[0].rstrip('/')
                            if url == company_url:
                                row_index = i
                                break
                        elif part.startswith('company_id:'):
                            company_id = part.replace('company_id:', '').strip()
                            url = f"https://www.linkedin.com/company/{company_id}"
                            if url == company_url:
                                row_index = i
                                break
                    if row_index != -1:
                        break
                        
                if row_index == -1:
                    print(f"❌ Could not find matching row for company URL: {company_url}")
                    continue
            else:
                # For profile data, use input_url as before
                input_url = data.get('input_url')
                if not input_url:
                    continue
                    
                # Find the column index for the LinkedIn URL
                url_column_index = headers.index(GOOGLE_SHEETS['column_with_links']) + 1
                
                # Get all URLs from the sheet
                urls = worksheet.col_values(url_column_index)
                
                try:
                    row_index = urls.index(input_url) + 1  # +1 because gspread uses 1-based indexing
                except ValueError:
                    print(f"URL not found in sheet: {input_url}")
                    continue
            
            # Prepare data for update
            update_data = {}
            ordered_fields = {}
            other_fields = {}
            
            for key, value in data.items():
                # Skip URL fields and similar profiles data
                if key in ['input', 'url', 'similar_profiles', 'people_also_viewed']:
                    continue
                    
                # Format the value into a human-readable string
                formatted_value = format_company_value(value) if is_company else format_value(value)
                
                # Add 'enriched_' prefix to company fields
                if is_company:
                    key = f'enriched_{key}'
                
                # Sort fields into ordered and other categories
                if key in column_order:
                    ordered_fields[key] = (column_order[key], formatted_value)
                else:
                    other_fields[key] = formatted_value
            
            # Sort ordered fields by their priority
            sorted_fields = sorted(ordered_fields.items(), key=lambda x: x[1][0])
            
            # Add ordered fields first
            for key, (priority, value) in sorted_fields:
                # Find or create column for this field
                try:
                    col_index = headers.index(key) + 1
                except ValueError:
                    # Add new column if it doesn't exist
                    worksheet.add_cols(1)
                    col_index = len(headers) + 1
                    worksheet.update_cell(1, col_index, key)
                    headers.append(key)
                    time.sleep(SHEETS_UPDATE_DELAY)  # Add delay after adding column
                
                update_data[col_index] = value
            
            # Add other fields after ordered ones
            for key, value in other_fields.items():
                try:
                    col_index = headers.index(key) + 1
                except ValueError:
                    worksheet.add_cols(1)
                    col_index = len(headers) + 1
                    worksheet.update_cell(1, col_index, key)
                    headers.append(key)
                    time.sleep(SHEETS_UPDATE_DELAY)
                
                update_data[col_index] = value
            
            # Update the row in batches
            cell_updates = []
            for col_index, value in update_data.items():
                cell_updates.append({
                    'range': f"{gspread.utils.rowcol_to_a1(row_index, col_index)}",
                    'values': [[value]]
                })
                
                # If we've reached batch size, update and clear the batch
                if len(cell_updates) >= BATCH_SIZE:
                    worksheet.batch_update(cell_updates)
                    cell_updates = []
                    time.sleep(SHEETS_UPDATE_DELAY)  # Add delay between batches
            
            # Update any remaining cells
            if cell_updates:
                worksheet.batch_update(cell_updates)
                time.sleep(SHEETS_UPDATE_DELAY)  # Add delay after final batch
            
            print(f"✅ Updated row for {'company' if is_company else 'profile'} URL: {company_url if is_company else input_url}")
            
        # Mark this snapshot as updated in Google Sheet
        updated_snapshots = load_updated_snapshots(is_company)
        updated_snapshots.add(snapshot_id)
        save_updated_snapshots(updated_snapshots, is_company)
            
    except Exception as e:
        print(f"❌ Error updating Google Sheet: {str(e)}")
        if "Quota exceeded" in str(e):
            print("⚠️ Google Sheets API quota exceeded. Waiting 60 seconds before retrying...")
            time.sleep(60)  # Wait 60 seconds before retrying
            return update_google_sheet(snapshot_data, snapshot_id, is_company)  # Retry the update

def update_similar_companies(snapshot_data, snapshot_id):
    """Add similar companies URLs to a separate worksheet."""
    try:
        client = get_google_sheet_client()
        sheet = client.open(GOOGLE_SHEETS['sheet_name'])
        
        # Try to get the 'Similar Companies' worksheet, create if it doesn't exist
        try:
            worksheet = sheet.worksheet('Similar Companies')
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sheet.add_worksheet(title='Similar Companies', rows=1000, cols=14)  # 4 main columns + 10 extra
            
        # Check if headers exist, add them if they don't
        current_headers = worksheet.row_values(1)
        if not current_headers or current_headers[0] != 'Company_url':
            headers = ['Company_url', 'name', 'industry', 'location'] + [''] * 10  # 4 main headers + 10 empty
            worksheet.update([headers], 'A1:N1')  # Update headers for all 14 columns
        
        # Get existing URLs from the sheet
        existing_urls = set(worksheet.col_values(1)[1:])  # Skip header row
        print(f"📊 Found {len(existing_urls)} existing company URLs in sheet")
        
        # Collect all rows to update
        rows_to_update = []
        seen_urls = set()  # Track unique URLs
        
        # Process each company in the snapshot
        for company in snapshot_data:
            similar_companies = company.get('similar', [])
            for similar in similar_companies:
                url = similar.get('Links', '')
                name = similar.get('title', '')
                industry = similar.get('subtitle', '')
                location = similar.get('location', '')
                
                # Normalize URL by removing tracking parameters
                url = url.split('?')[0].rstrip('/')
                
                if url and url not in seen_urls and url not in existing_urls:
                    seen_urls.add(url)
                    # Create row with 4 main fields + 10 empty fields
                    row = [url, name, industry, location] + [''] * 10
                    rows_to_update.append(row)
        
        # Update in batches
        if rows_to_update:
            print(f"📝 Found {len(rows_to_update)} new unique similar companies to add...")
            # Get the next empty row
            next_row = len(worksheet.get_all_values()) + 1
            # Update all rows at once for all 14 columns
            worksheet.update(values=rows_to_update, range_name=f'A{next_row}:N{next_row + len(rows_to_update) - 1}')
            print(f"✅ Added {len(rows_to_update)} new unique similar companies to sheet")
        else:
            print("ℹ️ No new similar companies to add")
            
    except Exception as e:
        print(f"❌ Error adding similar company URLs: {str(e)}")
        if "Quota exceeded" in str(e):
            print("⚠️ Google Sheets API quota exceeded. Waiting 60 seconds before retrying...")
            time.sleep(60)  # Wait 60 seconds before retrying
            return update_similar_companies(snapshot_data, snapshot_id)  # Retry the update

def process_snapshot_file(file_path, is_company=False):
    """Process a single snapshot file and update Google Sheet."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            snapshot_data = json.load(f)
        
        snapshot_id = os.path.basename(file_path).replace('.json', '')
        
        if is_company:
            # Update main sheet with company data
            update_google_sheet(snapshot_data, snapshot_id, is_company=True)
            # Update similar companies
            update_similar_companies(snapshot_data, snapshot_id)
        else:
            # Update main sheet
            update_google_sheet(snapshot_data, snapshot_id)
            # Update similar profiles
            update_similar_profiles(snapshot_data, snapshot_id)
        
    except Exception as e:
        print(f"❌ Error processing snapshot file {file_path}: {str(e)}")

def process_pending_updates():
    """Process any snapshots that are downloaded but not yet updated in Google Sheet."""
    if not os.path.exists(PROFILE_SAVE_DIR) and not os.path.exists(COMPANY_SAVE_DIR):
        return
        
    # Load lists of processed and updated snapshots
    processed_snapshots = load_processed_snapshots(is_company=False)
    updated_snapshots = load_updated_snapshots(is_company=False)
    
    # Find snapshots that need processing
    pending_snapshots = []
    for filename in os.listdir(PROFILE_SAVE_DIR):
        if not filename.endswith('.json'):
            continue
            
        snapshot_id = filename.replace('.json', '')
        if snapshot_id in processed_snapshots and snapshot_id not in updated_snapshots:
            pending_snapshots.append(os.path.join(PROFILE_SAVE_DIR, filename))
    
    for filename in os.listdir(COMPANY_SAVE_DIR):
        if not filename.endswith('.json'):
            continue
            
        snapshot_id = filename.replace('.json', '')
        if snapshot_id in processed_snapshots and snapshot_id not in updated_snapshots:
            pending_snapshots.append(os.path.join(COMPANY_SAVE_DIR, filename))
    
    if pending_snapshots:
        print(f"\n🔄 Found {len(pending_snapshots)} snapshots pending Google Sheet update")
        for file_path in pending_snapshots:
            print(f"Processing pending snapshot: {os.path.basename(file_path)}")
            process_snapshot_file(file_path)
            time.sleep(SHEETS_UPDATE_DELAY)  # Add delay between processing snapshots
        print("✅ Completed processing pending snapshots\n")

def ensure_directories():
    """Ensure required directories exist."""
    os.makedirs(PROFILE_SAVE_DIR, exist_ok=True)
    os.makedirs(COMPANY_SAVE_DIR, exist_ok=True)

def load_processed_snapshots(is_company=False):
    """Load the list of already processed snapshots."""
    processed_file = COMPANY_PROCESSED_FILE if is_company else PROFILE_PROCESSED_FILE
    if os.path.exists(processed_file):
        with open(processed_file, "r") as f:
            return set(json.load(f))
    return set()

def save_processed_snapshots(processed_snapshots, is_company=False):
    """Save the list of processed snapshots to file."""
    processed_file = COMPANY_PROCESSED_FILE if is_company else PROFILE_PROCESSED_FILE
    with open(processed_file, "w") as f:
        json.dump(list(processed_snapshots), f)

def get_snapshots(status=None, is_company=False):
    """Fetch snapshots with optional status filter.
    
    Args:
        status (str, optional): Filter snapshots by status
        is_company (bool): Whether to fetch company snapshots (True) or profile snapshots (False)
    """
    # Calculate the date threshold
    threshold_date = datetime.utcnow() - timedelta(days=BRIGHT_DATA['lookback_days'])
    threshold_str = threshold_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    
    # Use appropriate dataset ID based on type
    dataset_id = BRIGHT_DATA['company_dataset_id'] if is_company else BRIGHT_DATA['profile_dataset_id']
    if not dataset_id:
        raise ValueError(f"Dataset ID not found in config for {'company' if is_company else 'profile'} scraping")
    
    params = {
        "dataset_id": dataset_id,
        "from_date": threshold_str
    }
    if status:
        params["status"] = status
    
    print(f"🔍 Fetching {'company' if is_company else 'profile'} snapshots with dataset ID: {dataset_id}")
    response = requests.get(SNAPSHOTS_LIST_URL, headers=HEADERS, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"❌ Failed to fetch snapshots: {response.status_code}")
        return []

def download_snapshot(snapshot_id, is_company=False):
    """Download and save a snapshot."""
    save_dir = COMPANY_SAVE_DIR if is_company else PROFILE_SAVE_DIR
    response = requests.get(
        f"{SNAPSHOT_FETCH_URL}{snapshot_id}",
        headers=HEADERS,
        params=SNAPSHOT_PARAMS
    )
    
    if response.status_code == 200:
        snapshot_data = response.json()
        save_path = os.path.join(save_dir, f"{snapshot_id}.json")
        with open(save_path, "w", encoding="utf-8") as f:
            json.dump(snapshot_data, f, indent=2)
        return True, save_path
    else:
        return False, f"Failed to fetch snapshot: {snapshot_id}, status: {response.status_code}"

def process_profile_snapshots():
    """Process profile snapshots."""
    print("\n👤 Starting profile snapshot processing...")
    processed_snapshots = load_processed_snapshots(is_company=False)
    print(f"📋 Found {len(processed_snapshots)} previously processed profile snapshots")
    
    while True:
        # Get profile snapshots
        snapshots = get_snapshots(is_company=False)
        if not snapshots:
            print("No profile snapshots found. Waiting...")
            time.sleep(30)
            continue
        
        # Filter out already processed snapshots
        new_snapshots = [s for s in snapshots if s.get("id") not in processed_snapshots]
        if not new_snapshots:
            print("No new profile snapshots to process. Waiting...")
            time.sleep(30)
            continue
        
        # Check for running snapshots
        running_snapshots = [s for s in new_snapshots if s.get("status") == "running"]
        if running_snapshots:
            print(f"⏳ {len(running_snapshots)} profile snapshots still running...")
        
        # Process ready snapshots
        ready_snapshots = [s for s in new_snapshots if s.get("status") == "ready"]
        if not ready_snapshots:
            print("No ready profile snapshots to process. Waiting...")
            time.sleep(30)
            continue
        
        print(f"📥 Found {len(ready_snapshots)} new ready profile snapshots to process")
        for snapshot in ready_snapshots:
            snapshot_id = snapshot.get("id")
            if not snapshot_id:
                continue
                
            print(f"⬇️ Downloading profile snapshot: {snapshot_id}")
            success, result = download_snapshot(snapshot_id, is_company=False)
            
            if success:
                print(f"📁 Saved: {result}")
                # Process the snapshot immediately after download
                process_snapshot_file(result)
                processed_snapshots.add(snapshot_id)
                # Save after each successful processing
                save_processed_snapshots(processed_snapshots, is_company=False)
            else:
                print(f"❌ {result}")
        
        # If no running snapshots, we're done
        if not running_snapshots:
            print("✅ All profile snapshots processed!")
            break
        
        # Wait before next check
        time.sleep(30)

def process_company_snapshots():
    """Process company snapshots."""
    print("\n🏢 Starting company snapshot processing...")
    processed_snapshots = load_processed_snapshots(is_company=True)
    updated_snapshots = load_updated_snapshots(is_company=True)
    print(f"📋 Found {len(processed_snapshots)} previously processed company snapshots")
    print(f"📋 Found {len(updated_snapshots)} previously updated company snapshots")
    
    while True:
        # Get company snapshots
        snapshots = get_snapshots(is_company=True)
        if not snapshots:
            print("No company snapshots found. Waiting...")
            time.sleep(30)
            continue
        
        # Filter out already processed snapshots
        new_snapshots = [s for s in snapshots if s.get("id") not in processed_snapshots]
        if not new_snapshots:
            print("No new company snapshots to process. Waiting...")
            time.sleep(30)
            continue
        
        # Check for running snapshots
        running_snapshots = [s for s in new_snapshots if s.get("status") == "running"]
        if running_snapshots:
            print(f"⏳ {len(running_snapshots)} company snapshots still running...")
        
        # Process ready snapshots
        ready_snapshots = [s for s in new_snapshots if s.get("status") == "ready"]
        if not ready_snapshots:
            print("No ready company snapshots to process. Waiting...")
            time.sleep(30)
            continue
        
        print(f"📥 Found {len(ready_snapshots)} new ready company snapshots to process")
        for snapshot in ready_snapshots:
            snapshot_id = snapshot.get("id")
            if not snapshot_id:
                continue
                
            print(f"⬇️ Downloading company snapshot: {snapshot_id}")
            success, result = download_snapshot(snapshot_id, is_company=True)
            
            if success:
                print(f"📁 Saved: {result}")
                # Process the snapshot immediately after download
                process_snapshot_file(result, is_company=True)
                processed_snapshots.add(snapshot_id)
                # Save after each successful processing
                save_processed_snapshots(processed_snapshots, is_company=True)
            else:
                print(f"❌ {result}")
        
        # If no running snapshots, we're done
        if not running_snapshots:
            print("✅ All company snapshots processed!")
            break
        
        # Wait before next check
        time.sleep(30)

def score_lead(row_data):
    """Score a lead using OpenAI based on configured criteria."""
    try:
        # Extract required fields from row data
        field_values = {}
        for field in LEAD_SCORING['fields']:
            field_values[field] = row_data.get(field, '')
        
        # Format the prompt with actual values
        prompt = LEAD_SCORING['prompt'].format(
            position=field_values['position'],
            about=field_values['about'],
            website=field_values['enriched_website'],
            country_codes=field_values['enriched_country_codes'],
            company_about=field_values['enriched_unformatted_about'],
            crunchbase_url=field_values['enriched_crunchbase_url']
        )
        
        # Initialize OpenAI client
        openai.api_key = OPENAI['api_key']
        
        # Get score from OpenAI
        response = openai.ChatCompletion.create(
            model=OPENAI['model'],
            messages=[
                {"role": "system", "content": "You are an expert in global business analysis and language service consulting. Use given data and company press releases, News for research. Do not hallucinate."},
                {"role": "user", "content": prompt}
            ],
            timeout=OPENAI['timeout']
        )
        
        # Extract score from response
        score = response.choices[0].message.content.strip()
        try:
            score = float(score)
            return min(max(score, 0), 10)  # Ensure score is between 0-10
        except ValueError:
            print(f"❌ Invalid score received: {score}")
            return 0
            
    except Exception as e:
        print(f"❌ Error scoring lead: {str(e)}")
        return 0

def update_lead_scores():
    """Update lead scores for all rows in the sheet."""
    try:
        client = get_google_sheet_client()
        sheet = client.open(GOOGLE_SHEETS['sheet_name'])
        worksheet = sheet.worksheet(GOOGLE_SHEETS['worksheet_name'])
        
        # Get all data
        all_data = worksheet.get_all_values()
        headers = all_data[0]
        
        # Add lead_score column as second column if it doesn't exist
        if 'lead_score' not in headers:
            # Insert new column at position 2 (after URL column)
            worksheet.insert_cols([['lead_score']], 2)
            lead_score_col = 2
            headers.insert(1, 'lead_score')
        else:
            # If lead_score exists, move it to second position
            lead_score_col = headers.index('lead_score') + 1
            if lead_score_col != 2:
                # Move the column to second position
                worksheet.insert_cols([['lead_score']], 2)
                # Delete the old column
                worksheet.delete_columns(lead_score_col + 1, 1)
                lead_score_col = 2
                headers.remove('lead_score')
                headers.insert(1, 'lead_score')
        
        # Process each row (skip header)
        for i, row in enumerate(all_data[1:], start=2):  # Start from row 2
            # Create dictionary of row data
            row_data = dict(zip(headers, row))
            
            # Skip if already scored
            if row_data.get('lead_score'):
                continue
                
            print(f"🔍 Scoring lead {i-1}/{len(all_data)-1}")
            score = score_lead(row_data)
            
            # Update score in second column
            worksheet.update_cell(i, lead_score_col, score)
            time.sleep(SHEETS_UPDATE_DELAY)  # Respect rate limits
            
        print("✅ Lead scoring completed")
        
    except Exception as e:
        print(f"❌ Error updating lead scores: {str(e)}")

# def process_snapshots():
#     """Main function to monitor and process snapshots."""
#     ensure_directories()
    
#     # First process all profile snapshots
#     process_profile_snapshots()
    
#     # Check if current_company column exists before processing company snapshots
#     try:
#         client = get_google_sheet_client()
#         sheet = client.open(GOOGLE_SHEETS['sheet_name'])
#         worksheet = sheet.worksheet(GOOGLE_SHEETS['worksheet_name'])
#         headers = worksheet.row_values(1)
        
#         if 'current_company' in headers:
#             print("\n🏢 Found current_company column, starting company snapshot processing...")
#             process_company_snapshots()
#         else:
#             print("\nℹ️ current_company column not found yet. Company processing will start after profile processing is complete.")
            
#         # After all processing is complete, update lead scores
#         print("\n📊 Starting lead scoring...")
#         update_lead_scores()
        
#     except Exception as e:
#         print(f"❌ Error checking for current_company column: {str(e)}")
#         # Even if there's an error, try to update lead scores
#         try:
#             print("\n📊 Starting lead scoring...")
#             update_lead_scores()
#         except Exception as scoring_error:
#             print(f"❌ Error during lead scoring: {str(scoring_error)}")

# def main():
#     try:
#         process_snapshots()
#     except KeyboardInterrupt:
#         print("\n👋 Script stopped by user")
#     except Exception as e:
#         print(f"❌ An error occurred: {str(e)}")

# if __name__ == "__main__":
#     main()
