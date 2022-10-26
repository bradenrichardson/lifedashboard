import json
import requests
import os
import boto3

api_token = os.getenv('api_token')
api_url_base = 'https://api.up.com.au/api/v1/'
headers = {'Authorization': 'Bearer {}'.format(api_token)}

def write_to_dynamo(transaction_id):
    print(transaction_id)
    dynamodb = boto3.client('dynamodb')
    api_url = 'https://api.up.com.au/api/v1/transactions/' + transaction_id 
    response = requests.get(api_url, headers=headers)
    print(response)
    data = response.json()
    print(data)
    transaction = data.get('data')
    
    
    if transaction.get('relationships').get('category').get('data'):
        category = transaction.get('relationships').get('category').get('data').get('id')
    else:
        category = 'Uncategorized'
    if transaction.get('relationships').get('parentCategory').get('data'):
        parentCategory = transaction.get('relationships').get('parentCategory').get('data').get('id')
    else:
        parentCategory = 'Uncategorized'
    dynamodb.put_item(TableName='up_banking_2023', Item={'id':{'S': transaction.get('id')},'Category':{'S': category}, 
    'ParentCategory' : {'S' : parentCategory}, 'Value' : {'N' : transaction.get('attributes').get('amount').get('value')}, 'FinalCategory' : {'S' : 'TagMe'}, 'FinalSubCategory' : {'S' : 'TagMe'}, 'Description' : {'S' : transaction.get('attributes').get('description')}, 
    'CreatedAt' : {'S' : transaction.get('attributes').get('createdAt')}})



def lambda_handler(event, context):
    transaction_id = event.get('Records')[0].get('body')
    write_to_dynamo(transaction_id)