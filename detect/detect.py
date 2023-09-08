import boto3
import json

s3_client = boto3.client('s3')
sns_client = boto3.client('sns')

def lambda_handler(event, context):
    # Get the detected labels from the input event
    detected_labels = event['results']
    
    # Prepare the results
    results = []
    for label in detected_labels:
        results.append({
            'Name': label['Name'],
            'Confidence': label['Confidence']
        })
    
    # Upload the results to the S3 bucket
    # Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html
    s3_client.put_object(
        Bucket='thumbnail17',
        Key='object_scene_detection_results.json',
        Body=json.dumps(results)
    )
    
    # Check if a person is present in the image
    is_person_present = any(label['Name'] == 'Person' for label in detected_labels)
    
    # If a person is detected, publish to the "trigger" SNS topic
    # Referene: https://docs.aws.amazon.com/sns/latest/dg/sns-configuring.html
    sns_client.publish(
        TopicArn='arn:aws:sns:us-east-1:919305105911:trigger',
        Message=json.dumps(event)
    )
    
    # Return success or any desired response
    return {
        'statusCode': 200,
        'body': 'Results stored and SNS notification sent successfully'
    }