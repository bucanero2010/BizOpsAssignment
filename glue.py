import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

def sanitize_column_name(name):
    # write this function yourself

sheets_service = build('sheets', 'v4')
sheet = sheets_service.spreadsheets()
sheet_ranges = [
    {
        'tab': 'Users',
        'location': 'A1:E'
    }
]

for r in sheet_ranges:
    result = sheet.values().get(
        spreadsheetId='1iOgTfqYuqQtmOjPLZkSWjA_qU07s4NCWVGzEP6jfT0k',
        range=r['location']
    ).execute()
    data = result.get('values')
    d = pd.DataFrame(data[1:])
    d.columns = [sanitize_column_name(name) for name in data[0]]
    write_to_redshift(data=d, tablename=sanitize_column_name(r['tab']))
