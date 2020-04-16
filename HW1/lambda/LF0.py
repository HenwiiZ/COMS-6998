import json
import boto3

def lambda_handler(event, context):
    # TODO implement
    msg = event['messages'][0]['unstructured']['text']
    client = boto3.client('lex-runtime')
    response = client.post_text(
        botName='Dining',
        botAlias='dining',
        userId='id',
        sessionAttributes={},
        requestAttributes={},
        inputText=msg
    )
    
    return {
        'statusCode': 200,
        'body': response["message"]
    }
