import boto3
import json

# Reference: https://docs.aws.amazon.com/rekognition/latest/dg/what-is.html

rekognition_client = boto3.client('rekognition')
s3_client = boto3.client('s3')
sns_client = boto3.client('sns')

def lambda_handler(event, context):
    try:
        # Extract the S3 bucket and key from the input event
        message = json.loads(event['Records'][0]['Sns']['Message'])
        bucket = message['bucket']
        key = message['key']
    
        # Fetching the images from the specified S3 bucket
        response = s3_client.get_object(
            Bucket=bucket,
            Key=key
        )
        
        # Get the image bytes
        image_bytes = response['Body'].read()
        
        # Perform face mask detection using Rekognition
        # Reference: https://docs.aws.amazon.com/rekognition/latest/dg/ppe-detection.html
        response = rekognition_client.detect_protective_equipment(
            Image={
                'Bytes': image_bytes
            },
            SummarizationAttributes={
                'MinConfidence': 80,
                'RequiredEquipmentTypes': ['FACE_COVER']
            }
        )
        
        # Check if response contains 'Persons' key and non-empty list
        is_face_mask_detected = False
        if 'Persons' in response and isinstance(response['Persons'], list):
            # Check if face mask is detected
            for person in response['Persons']:
                for body_part in person.get('BodyParts', []):
                    if body_part['Name'] == 'FACE':
                        for detection in body_part.get('EquipmentDetections', []):
                            if detection['Type'] == 'FACE_COVER' and detection['CoversBodyPart']['Value']:
                                is_face_mask_detected = True
                                break
                if is_face_mask_detected:
                    break
        
        # Prepare the result
        result = {
            'bucket': bucket,
            'key': key,
            'is_face_mask_detected': is_face_mask_detected
        }
        
        # Store the result in the "user-video1" S3 bucket as a JSON file
        # Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html
        s3_client.put_object(
            Bucket='user-video1',
            Key=f'{key}.json',
            Body=json.dumps(result)
        )
        
        # Upload the image to the "user-video1" S3 bucket if face mask is detected
        if is_face_mask_detected:
            s3_client.put_object(
                Bucket='user-video1',
                Key=key,
                Body=image_bytes
            )
        
        # If face mask is detected, publish to the "trigger" SNS topic with the bucket name and key in the message
        if is_face_mask_detected:
            sns_client.publish(
                TopicArn='arn:aws:sns:us-east-1:919305105911:trigger',
                Message=json.dumps({
                    'bucketName': bucket,
                    'objectKey': key
                })
            )
        
        # Return the result
        return result
    except Exception as e:
        # Handle the timeout event
        if 'Task timed out' in str(e):
            return {
                'errorMessage': 'TimeoutError',
                'errorType': 'TimeoutError'
            }
        else:
            # Handle other exceptions
            return {
                'errorMessage': str(e),
                'errorType': type(e).__name__
            }
