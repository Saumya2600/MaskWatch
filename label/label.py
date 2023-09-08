import boto3
# Reference: https://docs.aws.amazon.com/rekognition/latest/dg/what-is.html
rekognition_client = boto3.client('rekognition')
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # Get the S3 bucket and key from the event
    bucket = event['detail']['bucket']['name']
    key = event['detail']['object']['key']
    
    # Fetch the image from the specified S3 bucket
    # Reference: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_object.html
    response = s3_client.get_object(
        Bucket=bucket,
        Key=key
    )
    
    # Get the image bytes
    image_bytes = response['Body'].read()
    
    # Perform object and scene detection using Rekognition
    #  Reference: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_object.html
    response = rekognition_client.detect_labels(
        Image={
            'Bytes': image_bytes
        }
    )
    
    # Extract the detected labels from the response
    detected_labels = response['Labels']
    
    # Prepare the results
    results = []
    for label in detected_labels:
        results.append({
            'Name': label['Name'],
            'Confidence': label['Confidence']
        })
    
    # Return the detected labels as the Lambda function output
    return {
        'bucket': bucket,
        'key': key,
        'results': results
    }