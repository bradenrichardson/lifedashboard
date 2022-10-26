from __future__ import print_function
import datetime
from os import error
import os.path
import json
from requests import api
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import boto3
import re

## TODO: Catch invalid_grant: Token has been expired or revoked and delete token.json


## ------------------------------------------------------------------------------ ##

dynamodb_table_name = os.environ['dynamodb_table_name']
time_max = os.environ['time_max']


def get_calendar_event_ids():
    SCOPES = ['https://www.googleapis.com/auth/calendar.readonly']
    try:
        creds = None
        if os.path.exists('token.json'):
            creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        service = build('calendar', 'v3', credentials=creds)

        start = datetime.datetime.utcnow().isoformat() + 'Z'

        events_result = service.events().list(calendarId='primary', timeMin=start, timeMax=time_max,
                                            maxResults=2500, singleEvents=True,
                                            orderBy='startTime').execute()
        events = events_result.get('items', [])
        
        data = []
        
        if not events:
            print('No upcoming events found.')
        for event in events:
                data.append(
                    event['id']
                )
        return data        
            
    except error as E:
        print(E)


## ------------------------------------------------------------------------------ ##


def clean_events():
    calendar_event_ids = get_calendar_event_ids()
    
    dynamodb = boto3.resource('dynamodb',
                          region_name='ap-southeast-2'
                )

    table = dynamodb.Table(dynamodb_table_name)
    dynamodb_client = boto3.client('dynamodb')

    response = table.scan()
    data = response['Items']

    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])
    
    for item in data:
        if item.get('id') not in calendar_event_ids:
            response = dynamodb_client.delete_item(TableName=dynamodb_table_name, Key={
                    'id' : { 
                        'S': item.get('id')
                            }
                        })
            print(response)



## ------------------------------------------------------------------------------ ##


def put_events():
    SCOPES = ['https://www.googleapis.com/auth/calendar.readonly']
    try:
        dynamodb = boto3.client('dynamodb')
        creds = None
        if os.path.exists('token.json'):
            creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        service = build('calendar', 'v3', credentials=creds)

        start = datetime.datetime.utcnow().isoformat() + 'Z'

        events_result = service.events().list(calendarId='primary', timeMin=start, timeMax=time_max,
                                            maxResults=2500, singleEvents=True,
                                            orderBy='startTime').execute()
        events = events_result.get('items', [])
        
        data = []
        
        if not events:
            print('No upcoming events found.')
        for event in events:
            if re.findall('\((.*?)\)', event['summary']):
                if 'Income' not in event['summary'] and 'Due' not in event['summary'] and 'Spending' not in event['summary'] and 'Savings' not in event['summary'] and 'Debt' not in event['summary']:
                    continue

                if 'Income' in event['summary']:
                    value = re.findall('[0-9]+', event['summary'])
                    type = 'Income'

                if 'Due' in event['summary']:
                    value = re.findall('[0-9]+', event['summary'])
                    type = 'Bills'

                if 'Debt' in event['summary']:
                    value = re.findall('[0-9]+', event['summary'])
                    type = 'Debt'
                
                if 'Spending' in event['summary']:
                    value = re.findall('[0-9]+', event['summary'])
                    type = 'Spending'
                
                if 'Savings' in event['summary']:
                    value = re.findall('[0-9]+', event['summary'])
                    type = 'Savings'

                title = re.findall('\((.*?)\)', event['summary'])
                dynamodb.put_item(TableName=dynamodb_table_name, Item={
                    
                    'id':{'S': event['id']},
                    'date':{'S': event['start'].get('date')},
                    'type':{'S': type},
                    'value':{'N': value[0]},
                    'title':{'S': title[0]}
                        })

    except error as E:
        print(E)


## ------------------------------------------------------------------------------ ##


def lambda_handler(event, context):
    clean_events()
    put_events()