import requests
import os
import json
import time
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from config import GOOGLE_SHEETS, BRIGHT_DATA

# Constants
SNAPSHOTS_LIST_URL = "https://api.brightdata.com/datasets/v3/snapshots"
SNAPSHOT_FETCH_URL = "https://api.brightdata.com/datasets/v3/snapshot/"
HEADERS = {"Authorization": f"Bearer {BRIGHT_DATA['api_key']}"}
SNAPSHOT_PARAMS = {"format": "json"}
SAVE_DIR = "snapshots_downloaded"
PROCESSED_FILE = "processed_snapshots.json"
UPDATED_FILE = "updated_snapshots.json"

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

def load_updated_snapshots():
    """Load the list of snapshots already updated in Google Sheet."""
    if os.path.exists(UPDATED_FILE):
        with open(UPDATED_FILE, "r") as f:
            return set(json.load(f))
    return set()

def save_updated_snapshots(updated_snapshots):
    """Save the list of snapshots updated in Google Sheet."""
    with open(UPDATED_FILE, "w") as f:
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

def update_google_sheet(snapshot_data, snapshot_id):
    """Update Google Sheet with snapshot data."""
    try:
        client = get_google_sheet_client()
        sheet = client.open(GOOGLE_SHEETS['sheet_name'])
        worksheet = sheet.worksheet(GOOGLE_SHEETS['worksheet_name'])
        
        # Get all values to find the row with matching URL
        all_values = worksheet.get_all_values()
        headers = all_values[0]
        
        # Find the column index for the LinkedIn URL
        url_column_index = headers.index(GOOGLE_SHEETS['column_with_links']) + 1
        
        # Get all URLs from the sheet
        urls = worksheet.col_values(url_column_index)
        
        # Get column order
        column_order = get_column_order()
        
        # Process each profile in the snapshot
        for profile in snapshot_data:
            input_url = profile.get('input_url')
            if not input_url:
                continue
                
            # Find the row with matching URL
            try:
                row_index = urls.index(input_url) + 1  # +1 because gspread uses 1-based indexing
            except ValueError:
                print(f"URL not found in sheet: {input_url}")
                continue
            
            # Prepare data for update
            update_data = {}
            ordered_fields = {}
            other_fields = {}
            
            # Split name into first and last name
            full_name = profile.get('name', '')
            first_name = ''
            last_name = ''
            if full_name:
                name_parts = full_name.split(' ', 1)
                first_name = name_parts[0]
                last_name = name_parts[1] if len(name_parts) > 1 else ''
            
            # Add name fields to ordered fields
            ordered_fields['first_name'] = (1, first_name)  # High priority
            ordered_fields['last_name'] = (2, last_name)    # High priority
            
            for key, value in profile.items():
                # Skip URL fields, similar profiles data, and name (since we split it)
                if key in ['input_url', 'url', 'similar_profiles', 'people_also_viewed', 'name']:
                    continue
                    
                # Format the value into a human-readable string
                formatted_value = format_value(value)
                
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
            
            print(f"✅ Updated row for URL: {input_url}")
            
        # Mark this snapshot as updated in Google Sheet
        updated_snapshots = load_updated_snapshots()
        updated_snapshots.add(snapshot_id)
        save_updated_snapshots(updated_snapshots)
            
    except Exception as e:
        print(f"❌ Error updating Google Sheet: {str(e)}")
        if "Quota exceeded" in str(e):
            print("⚠️ Google Sheets API quota exceeded. Waiting 60 seconds before retrying...")
            time.sleep(60)  # Wait 60 seconds before retrying
            return update_google_sheet(snapshot_data, snapshot_id)  # Retry the update

def process_snapshot_file(file_path):
    """Process a single snapshot file and update Google Sheet."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            snapshot_data = json.load(f)
        
        snapshot_id = os.path.basename(file_path).replace('.json', '')
        # Update main sheet
        update_google_sheet(snapshot_data, snapshot_id)
        # Update similar profiles
        update_similar_profiles(snapshot_data, snapshot_id)
        
    except Exception as e:
        print(f"❌ Error processing snapshot file {file_path}: {str(e)}")

def process_pending_updates():
    """Process any snapshots that are downloaded but not yet updated in Google Sheet."""
    if not os.path.exists(SAVE_DIR):
        return
        
    # Load lists of processed and updated snapshots
    processed_snapshots = load_processed_snapshots()
    updated_snapshots = load_updated_snapshots()
    
    # Find snapshots that need processing
    pending_snapshots = []
    for filename in os.listdir(SAVE_DIR):
        if not filename.endswith('.json'):
            continue
            
        snapshot_id = filename.replace('.json', '')
        if snapshot_id in processed_snapshots and snapshot_id not in updated_snapshots:
            pending_snapshots.append(os.path.join(SAVE_DIR, filename))
    
    if pending_snapshots:
        print(f"\n🔄 Found {len(pending_snapshots)} snapshots pending Google Sheet update")
        for file_path in pending_snapshots:
            print(f"Processing pending snapshot: {os.path.basename(file_path)}")
            process_snapshot_file(file_path)
            time.sleep(SHEETS_UPDATE_DELAY)  # Add delay between processing snapshots
        print("✅ Completed processing pending snapshots\n")

def ensure_directories():
    """Ensure required directories exist."""
    os.makedirs(SAVE_DIR, exist_ok=True)

def load_processed_snapshots():
    """Load the list of already processed snapshots."""
    if os.path.exists(PROCESSED_FILE):
        with open(PROCESSED_FILE, "r") as f:
            return set(json.load(f))
    return set()

def save_processed_snapshots(processed_snapshots):
    """Save the list of processed snapshots to file."""
    with open(PROCESSED_FILE, "w") as f:
        json.dump(list(processed_snapshots), f)

def get_snapshots(status=None):
    """Fetch snapshots with optional status filter."""
    # Calculate the date threshold
    threshold_date = datetime.utcnow() - timedelta(days=BRIGHT_DATA['lookback_days'])
    threshold_str = threshold_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    
    params = {
        "dataset_id": BRIGHT_DATA['dataset_id'],
        "from_date": threshold_str
    }
    if status:
        params["status"] = status
    
    response = requests.get(SNAPSHOTS_LIST_URL, headers=HEADERS, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"❌ Failed to fetch snapshots: {response.status_code}")
        return []

def download_snapshot(snapshot_id):
    """Download and save a snapshot."""
    response = requests.get(
        f"{SNAPSHOT_FETCH_URL}{snapshot_id}",
        headers=HEADERS,
        params=SNAPSHOT_PARAMS
    )
    
    if response.status_code == 200:
        snapshot_data = response.json()
        save_path = os.path.join(SAVE_DIR, f"{snapshot_id}.json")
        with open(save_path, "w", encoding="utf-8") as f:
            json.dump(snapshot_data, f, indent=2)
        return True, save_path
    else:
        return False, f"Failed to fetch snapshot: {snapshot_id}, status: {response.status_code}"

def process_snapshots():
    """Main function to monitor and process snapshots."""
    ensure_directories()
    processed_snapshots = load_processed_snapshots()
    
    # First, process any pending updates
    process_pending_updates()
    
    # Print the date range we're looking at
    threshold_date = datetime.utcnow() - timedelta(days=BRIGHT_DATA['lookback_days'])
    print(f"🔍 Looking for snapshots created after: {threshold_date.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    
    while True:
        # Get all snapshots
        snapshots = get_snapshots()
        if not snapshots:
            print("No snapshots found. Waiting...")
            time.sleep(30)  # Wait 30 seconds before checking again
            continue
        
        # Check for running snapshots
        running_snapshots = [s for s in snapshots if s.get("status") == "running"]
        if running_snapshots:
            print(f"⏳ {len(running_snapshots)} snapshots still running...")
        
        # Process ready snapshots
        ready_snapshots = [s for s in snapshots if s.get("status") == "ready"]
        new_snapshots = []
        
        for snapshot in ready_snapshots:
            snapshot_id = snapshot.get("id")
            if not snapshot_id or snapshot_id in processed_snapshots:
                continue
                
            print(f"⬇️ Downloading snapshot: {snapshot_id}")
            success, result = download_snapshot(snapshot_id)
            
            if success:
                print(f"📁 Saved: {result}")
                # Process the snapshot immediately after download
                process_snapshot_file(result)
                processed_snapshots.add(snapshot_id)
                new_snapshots.append(snapshot_id)
            else:
                print(f"❌ {result}")
        
        # Update processed snapshots file if we have new ones
        if new_snapshots:
            save_processed_snapshots(processed_snapshots)
            print(f"✅ Updated processed list with {len(new_snapshots)} new snapshots.")
        
        # If no running snapshots and all ready ones are processed, we're done
        if not running_snapshots and not [s for s in snapshots if s.get("status") == "ready" and s.get("id") not in processed_snapshots]:
            print("✅ All snapshots processed!")
            break
        
        # Wait before next check
        time.sleep(30)  # Check every 30 seconds

def main():
    try:
        process_snapshots()
    except KeyboardInterrupt:
        print("\n👋 Script stopped by user")
    except Exception as e:
        print(f"❌ An error occurred: {str(e)}")

if __name__ == "__main__":
    main()
