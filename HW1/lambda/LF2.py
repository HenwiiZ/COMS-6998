import json
import boto3
from botocore.vendored import requests
from random import randint

def lambda_handler(event, context):
    print("Log LF2 trigger")
    client = boto3.client('sqs')
    queueUrl='Your SQS URL'
    response = client.receive_message(QueueUrl=queueUrl)
    
    
    # Check if messages in SQS
    if response.get('Messages') == None:
        print('No message')
        return {
            'statusCode': 200,
            'body': json.dumps('No message in SQS!')
        }
        
    # Get message
    msg = response['Messages'][0]
    MessageId = msg['MessageId']
    ReceiptHandle = msg['ReceiptHandle']
    client.delete_message(QueueUrl=queueUrl,ReceiptHandle=ReceiptHandle)
    slots = json.loads(msg['Body'])
    cuisine = slots['cuisine']
    numPeople = slots['numPeople']
    date = slots['date']
    diningtime = slots['time']
    phone = '+1'+slots['phone']
    
    # Get result from elastic search
    region = 'us-east-1'
    service = 'es'
    access_key = 'Your AWS Access Key'
    secret_key = 'Your AWS Secret Access Key'
    
    host = 'Your ElasticSearch URL'
    index = 'restaurants'
    url = host + '/' + index + '/_search?q=' + cuisine
    
    r = requests.get(url)
    results = json.loads(r.text)['hits']['hits']
    idx = randint(0, len(results)-1)
    bus_id = results[idx]['_source']['Business ID']
    
    # Get info from dynamodb with business ID
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1',
                          aws_access_key_id='Your AWS Access Key',
                          aws_secret_access_key='Your AWS Secret Access Key')
    table = dynamodb.Table('yelp-restaurants')
    table_result = table.get_item(Key={"Business ID":bus_id})['Item']
    
    name = table_result['Name']
    address = table_result['Address']
    
    # Send SNS message
    sns_msg = 'Hello! Here are my {} restaurant suggestions for {} people, for {} at {}: {}, located at {}. Enjoy your meal!'.format(cuisine,numPeople,date,diningtime,name,address)
    print(sns_msg)
    snsclient = boto3.client('sns')
    response = snsclient.publish(
        PhoneNumber = phone,
        Message= sns_msg)
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
