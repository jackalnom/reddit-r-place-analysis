import boto3
import json
import sys

def detect_text(photo, bucket):
    session = boto3.Session(profile_name='default')
    client = session.client('rekognition')
    s3 = session.resource('s3')

    response = client.detect_text(Image={'S3Object': {'Bucket': bucket, 'Name': photo}})

    textDetections = response['TextDetections']
    high_confidence_texts = {}
    for text in textDetections:
        if text['Confidence'] >= 90 and not 'ParentId' in text:
            if text['DetectedText'] not in high_confidence_texts:
                high_confidence_texts[text['DetectedText']] = []
            high_confidence_texts[text['DetectedText']].append(str(text['Geometry']['BoundingBox']))

    # Save the high confidence texts to a JSON file
    with open('text_detected.json', 'w') as f:
        json.dump(high_confidence_texts, f)

    # Upload the JSON file to the S3 bucket
    s3.meta.client.upload_file('text_detected.json', bucket, 'text-detected/text_detected.json')

    return len(high_confidence_texts)

if __name__ == "__main__":
    photo = sys.argv[1]
    bucket = sys.argv[2]
    label_count = detect_text(photo, bucket)

    print("Labels detected: " + str(label_count))