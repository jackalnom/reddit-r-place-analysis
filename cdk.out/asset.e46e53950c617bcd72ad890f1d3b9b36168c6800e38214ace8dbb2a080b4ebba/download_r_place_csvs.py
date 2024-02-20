import boto3
import requests
from contextlib import closing
from concurrent.futures import ThreadPoolExecutor
import sys
from awsglue.utils import getResolvedOptions

job_args = getResolvedOptions(sys.argv, [
        'bucket',
       ])
# Initialize a boto3 client
s3 = boto3.client('s3')

def upload_file(file_number):
    base_url = 'https://placedata.reddit.com/data/canvas-history/2023/2023_place_canvas_history-{:012d}.csv.gzip'
    bucket_name = job_args['bucket']
    
    # Format the URL and the s3 file key with the current file number
    url = base_url.format(file_number)
    s3_file_key = f'csvs/2023_place_canvas_history-{file_number:012d}.csv.gz'
    
    with closing(requests.get(url, stream=True)) as response:
        # Ensure the request was successful
        response.raise_for_status()
        
        # Stream the file to S3 in chunks
        s3.upload_fileobj(response.raw, bucket_name, s3_file_key)
        print(f"Successfully uploaded file number {file_number}")

# List of file numbers to upload
file_numbers = range(0, 52)  # From 1 to 52 inclusive

# Use ThreadPoolExecutor to upload files in parallel
with ThreadPoolExecutor(max_workers=52) as executor:
    executor.map(upload_file, file_numbers)
