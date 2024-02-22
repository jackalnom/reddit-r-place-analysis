import boto3
import json
import sys

br = boto3.client('bedrock-runtime')

s3 = boto3.resource('s3')
bucket = sys.argv[1]
key = sys.argv[2]
output_key = sys.argv[3]

# Download the JSON file from the S3 bucket
s3.Bucket(bucket).download_file(key, 'text_detected.json')

# Load the JSON file into a Python dictionary
with open('text_detected.json', 'r') as f:
    detected_texts = json.load(f)
labels = []
for d in detected_texts:
    labels.append(d)
print(labels)

request = json.dumps({
    "inputText": f"""
        Given a list of labels extracted from text written by users of r/place 2023, 
        generate a JSON document where each entry consists of a label and a
        description of the community it represents. If multiple 
        labels refer to the same community, identify them as duplicates within their 
        descriptions. The format of the JSON document should be in array, with the label
        and the description of the label in each entry.
        The labels are: {labels}.

        JSON:
        """,
    "textGenerationConfig": {
        "maxTokenCount": 4096,
        "stopSequences": ["User:"],
        "temperature": 0,
        "topP": 1
    }
})

print(request)

response = br.invoke_model(
    modelId="amazon.titan-text-express-v1", body=request
)

response_body = json.loads(response["body"].read())["results"][0]["outputText"]
print(response_body)

with open('text_explained.json', 'w') as f:
    json.dump(response_body, f)

# Upload the JSON file to the S3 bucket
s3.meta.client.upload_file('text_explained.json', bucket, output_key)