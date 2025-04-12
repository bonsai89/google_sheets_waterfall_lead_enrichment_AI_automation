# Weavlog Lead Enricher


## Google Sheets Setup Instructions

### 1. Google Cloud Project Setup

1. Go to the [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select an existing one
3. Enable the required APIs:
   - In the left sidebar, click on "APIs & Services" > "Library"
   - Search for and enable both:
     - "Google Sheets API"
     - "Google Drive API"
   - For each API, click on it and click "Enable"

### 2. Create Service Account

1. In the Google Cloud Console:
   - Go to "IAM & Admin" > "Service Accounts"
   - Click "Create Service Account"
   - Fill in the service account details:
     - Service account name (e.g., "sheets-reader")
     - Service account ID (auto-generated)
     - Description (optional)
   - Click "Create and Continue"
   - Optional: Assign roles to your service account (for basic sheet access, you can skip this)
   - Click "Continue"
   - Optional: Add users who can manage this service account
   - Click "Done"

### 3. Create Service Account Key

1. In the service accounts list:
   - Find your newly created service account
   - Click on the service account name
   - Go to the "Keys" tab
   - Click "Add Key" > "Create new key"
   - Choose "JSON" format
   - Click "Create"
   - The JSON key file will automatically download to your computer

### 4. Set Up Domain-wide Delegation (Optional)

If you need to access data on behalf of users in your Google Workspace organization:

1. In the service account details:
   - Click "Show advanced settings"
   - Under "Domain-wide delegation", copy the "Client ID"
2. In the Google Admin console:
   - Go to "Security" > "Access and data control" > "API controls"
   - Click "Manage Domain Wide Delegation"
   - Click "Add new"
   - Paste the Client ID
   - Add required OAuth scopes:
     - `https://www.googleapis.com/auth/spreadsheets`
     - `https://www.googleapis.com/auth/drive`
   - Click "Authorize"

### 5. Rename and Move the Credentials File

1. Rename the downloaded JSON file to `credentials.json`
2. Move it to the root directory of this project

### 6. Share Your Google Sheet

1. Open your Google Sheet
2. Click the "Share" button in the top right
3. Add the service account email address (found in the `client_email` field of your `credentials.json`)
4. Give it "Editor" access
5. Click "Done"

### 7. Configure the Application

1. Open `config.py` and update the following settings:
   ```python
   GOOGLE_SHEETS = {
       'sheet_name': 'Your Sheet Name',
       'worksheet_name': 'Sheet1',
       'column_with_links': 'linkedin_person_url',
       'credentials_file': 'credentials.json'
   }
   ```

### 8. Install Required Packages

```bash
pip install gspread oauth2client pandas
```

### 9. Run the Application

```bash
python GS_enricher_main.py
```

## Security Notes

- Never commit `credentials.json` to version control
- Add `credentials.json` to your `.gitignore` file
- Keep your service account credentials secure
- Regularly rotate your service account keys
- Store credentials securely and follow the principle of least privilege
- Consider using Google Cloud Secret Manager for production environments

## Troubleshooting

If you encounter any issues:

1. Verify that both Google Sheets API and Google Drive API are enabled
2. Check that the service account has access to the sheet
3. Ensure the credentials file is in the correct location
4. Verify the sheet name and worksheet name in the config file
5. Check that the column name in the config matches your sheet's structure
6. If using domain-wide delegation, verify the OAuth scopes are correctly configured 