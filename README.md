# LinkedIn Lead Enrichment System

Clay style fully customizable waterfall lead enriching system that is 10x cheaper and scales efficiently fully hosted on Google sheets.

## Overview

This system automates the process of:
- Reading LinkedIn profile and company URLs from Google Sheets
- Enriching data using Bright Data's LinkedIn scraping capabilities and serperAI API for real-time webscraping and enriching.
- Scoring leads based on enrichment and custom GPT prompting and ready for integration with any Cold emailing infrastructure.

## Features

- **Profile Enrichment**: Automatically enriches LinkedIn profile data using openAI API and serper API
- **Company Enrichment**: Gathers detailed company information from Linkedin and Google search
- **Lead Scoring**: Updates lead scores based on enriched data and using custom AI prompting to your business usecase.
- **Batch Processing**: Handles large set of leads in chunks
- **Error Handling**: Robust error handling and retry mechanisms
- **Rate Limiting**: Respects API rate limits and GDPR rules
