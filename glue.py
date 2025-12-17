import sys
import time
import pandas as pd
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from googleapiclient.errors import HttpError

def sanitize_column_name(name):
    # write this function yourself
    a = ''

def find_sheet_range(sheet, spreadsheet_id, tab_name, search_value):
    '''
    Finds the start of the header based on the first column name, then finds the end by 
    looking for the first empty cell

    Returns: String ('A1:M') or None if it doesn't find any
    '''
    # Get all data from the tab
    result = sheet.values().get(
        spreadsheetId=spreadsheet_id,
        range=f"{tab_name}!A1:Z100"
    ).execute()

    data = result.get('values', [])

    # Search for the value
    for i, row in enumerate(data):
        for j, cell in enumerate(row):
            if cell == search_value:
                # Find starting cell
                start_col = j
                row_number = i + 1
            
                # Continue scanning row to find end of header
                end_col = start_col
                for k in range(j, len(row)):
                    if row[k] and row[k].strip():  # Non-empty cell
                        end_col = k
                    else:
                        break  # Found empty cell, stop
                
                # Convert to A1 Excel notation
                start_col_letter = chr(65 + start_col)
                end_col_letter = chr(65 + end_col)
                
                return f"{start_col_letter}{row_number}:{end_col_letter}"
    
    return None

def extract_sheet_data(sheet, spreadsheet_id, range_name, max_retries=3, base_delay=1):
    """
    Extract data from Google Sheet with retry logic
    """
    for attempt in range(max_retries):
        try:
            result = sheet.values().get(
                spreadsheetId=spreadsheet_id,
                range=range_name
            ).execute()
            return result.get('values',[])
        
        # Handle Google API errors
        except HttpError as e:
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)  # Exponential backoff
                print(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print(f"All {max_retries} attempts failed for range {range_name}")
                raise
        
        # Handle other errors
        except Exception as e:
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                print(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print(f"All {max_retries} attempts failed for range {range_name}")
                raise

# Initializing Sheets API
sheets_service = build('sheets', 'v4')
sheet = sheets_service.spreadsheets()
SPREADSHEET_ID = '1iOgTfqYuqQtmOjPLZkSWjA_qU07s4NCWVGzEP6jfT0k'


# Setting the tabs to be used
sheet_ranges = [
    {
        'tab': 'Users',
        'first_column': 'ID'
    },
    {
        'tab': 'Order Form',
        'first_column': 'Account ID'
    }
]


# Process each sheet
for r in sheet_ranges:
    print(f"Processing tab: {r['tab']}")
    
    try:
        # Get Sheet Range
        range_name = find_sheet_range(
            sheet=sheet,
            spreadsheet_id=SPREADSHEET_ID,
            tab_name=r['tab'],
            search_value=r['first_column']
        )

        # Extract data with retry logic
        data = extract_sheet_data(
            sheet=sheet,
            spreadsheet_id=SPREADSHEET_ID,
            range_name=range_name,
            max_retries=3,
            base_delay=1
        )
        
        # Convert to DataFrame
        d = pd.DataFrame(data[1:])

        # Sanitize columns
        d.columns = [sanitize_column_name(name) for name in data[0]]
        
        # Write to Redshift
        write_to_redshift(data=d, tablename=sanitize_column_name(r['tab']))
    
    except Exception as e:
        print(f"Failed to process tab {r['tab']}: {e}")
        raise
