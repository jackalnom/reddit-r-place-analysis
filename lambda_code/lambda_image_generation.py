import pandas as pd
import numpy as np
from PIL import Image
import logging
import io
import boto3
import awswrangler as wr
import time
import urllib
import re

def hex_to_rgb(value):
    value = value.lstrip('#')
    lv = len(value)
    return tuple(int(value[i:i + lv // 3], 16) for i in range(0, lv, lv // 3))

def read_latest_sql_file(bucket, key):
    # Initialize a boto3 client
    s3 = boto3.client('s3')
    
    # Read the content of the latest .sql file
    obj = s3.get_object(Bucket=bucket, Key=key)
    file_content = obj['Body'].read().decode('utf-8')
        
    return file_content


def lambda_handler(event, context):
    database = "r_place_db"
    workgroup = "primary"
    bucket_name = "reddit-rplace-analysis-sql"
    
    s3 = boto3.client('s3')
    
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    match = re.match(r'^queries/([^/]+)\.sql$', key)
    if match:
        query_name = match.group(1)
    else:
        raise Exception("SQL must be in queries folder")
    
    query = read_latest_sql_file(bucket, key)
    
    print(f"Running query: {query}")
    df = wr.athena.read_sql_query(sql=query, database=database, workgroup=workgroup)
    print("Query completed, read into DataFrame")
    
    if df.empty:
        return {"message": "No data returned from query."}
    
    grouped_df = df.groupby('date_bin')
    
    for date_bin, group in grouped_df:
        w, h = 3000, 2000
        data = np.zeros((h, w, 4), dtype=np.uint8)
        
        for index, row in group.iterrows():
            try:
                col, row, hex_color = int(row['y']), int(row['x']), row['pixel_color']
                data[col + 1000, row + 1500] = hex_to_rgb(hex_color) + (255,)
            except Exception as e:
                logging.error(f"Caught exception with row {index} - {str(e)}")
        
        img = Image.fromarray(data, 'RGBA')
        image_io = io.BytesIO()
        img.save(image_io, format='PNG')
        image_io.seek(0)
        
        s3_key = f"images/{query_name}/{date_bin}.png"
        s3.upload_fileobj(image_io, bucket_name, s3_key)
        print(f"Image saved to S3: {bucket_name}/{s3_key}")
    
    return {"message": "Images saved to S3."}
