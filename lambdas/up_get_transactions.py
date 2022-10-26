import json
import os
import boto3
import requests

api_token = os.getenv('api_token')

def write_to_dynamo(api_url):
    dynamodb = boto3.client('dynamodb')
    response = requests.get(api_url, headers={'Authorization': 'Bearer {}'.format(api_token)})
    print(response)
    if response.status_code == 200:
        data = []
        data.append(response.json().get('data'))
        for array in data:
            for transaction in array:
                if transaction.get('relationships').get('category').get('data'):
                    category = transaction.get('relationships').get('category').get('data').get('id')
                else:
                    category = 'Uncategorized'
                if transaction.get('relationships').get('parentCategory').get('data'):
                    parentCategory = transaction.get('relationships').get('parentCategory').get('data').get('id')
                else:
                    parentCategory = 'Uncategorized'
                dynamodb.put_item(TableName='up_banking_2023', Item={'id':{'S': transaction.get('id')},'Category':{'S': category}, 
                'ParentCategory' : {'S' : parentCategory}, 'FinalCategory' : {'S' : 'TagMe'}, 'FinalSubCategory' : {'S' : 'TagMe'}, 'Value' : {'N' : transaction.get('attributes').get('amount').get('value')}, 'Description' : {'S' : transaction.get('attributes').get('description')}, 
                'CreatedAt' : {'S' : transaction.get('attributes').get('createdAt')}})
        if response.json().get('links').get('next'):
            token = response.json().get('links').get('next')
            print("Sending token to SQS queue for processing: {}".format(token))
            sqs = boto3.client('sqs')
            sqs.send_message(QueueUrl="https://sqs.ap-southeast-2.amazonaws.com/007576465237/up-banking-queue", MessageBody=token)
    else:
        print(response.status_code)

        


def lambda_handler(event, context):
    
    if event:
        write_to_dynamo(event.get('Records')[0].get('body'))
    else:
        write_to_dynamo('https://api.up.com.au/api/v1/transactions')
    


