# Google Sheets Configuration
GOOGLE_SHEETS = {
    'credentials_file': 'path/to/your/credentials.json',  # Path to your credentials file
    'sheet_name': 'LeadGen_Enrichment',  # Replace with your actual sheet name
    'worksheet_name': 'Sheet1',  # Replace with your worksheet name
    'column_with_links': 'linkedin_person_url'  # Column containing the links
}

# Bright Data Configuration
BRIGHT_DATA = {  # 0.0015$/API call
    'api_key': 'bd_1234567890abcdefghijklmnopqrstuvwxyz',  # Replace with your actual API key
    'profile_dataset_id': 'ds_9876543210abcdefghijklmnopqrstuvwxyz',  # For profile scraping
    'company_dataset_id': 'ds_abcdefghijklmnopqrstuvwxyz1234567890',  # For company scraping
    'lookback_days': 1
}

# OpenAI Configuration
OPENAI = {
    'api_key': "sk-1234567890abcdefghijklmnopqrstuvwxyz",
    'model': "gpt-4o-mini",  # ~0.00036$/AI call
    'timeout': 60
}

# Lead Scoring Configuration
LEAD_SCORING = {
    'prompt': """Analyze the following company information and determine if the company is likely to require multilingual translation services. 
And if the person is the right person to approach for cold email. Score from 0-10, 10 being highest match.

Use the following fields to make your assessment:
- Position: {position}
- About: {about}
- Website: {website}
- Country Codes: {country_codes}
- Company About: {company_about}
- Crunchbase URL: {crunchbase_url}

Return ONLY a single number between 0-10 as your response, nothing else.""",
    'fields': [
        'position',
        'about',
        'enriched_website',
        'enriched_country_codes',
        'enriched_unformatted_about',
        'enriched_crunchbase_url'
    ]
} 