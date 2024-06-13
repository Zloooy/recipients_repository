import os
import boto3
from boto3.dynamodb.conditions import Key

RECIPIENTS_TABLE = 'recipients'
EMAIL_COLUMN = 'email'
YDB_ADDRESS = os.environ.get("YDB_ADDRESS")
YDB_ACCESS_KEY_ID = os.environ.get("YDB_ACCESS_KEY_ID")
YDB_SECRET_ACCESS_KEY = os.environ.get("YDB_SECRET_ACCESS_KEY")
class RecipientsRepository:

    def __init__(self):
        self.database = boto3.resource(
            'dynamodb',
            endpoint_url = YDB_ADDRESS,
            region_name = "ru-central1",
            aws_access_key_id = YDB_ACCESS_KEY_ID,
            aws_secret_access_key = YDB_SECRET_ACCESS_KEY
        )
        matching_table = self.database.tables.filter(ExclusiveTableName=RECIPIENTS_TABLE, Limit=1,)
        if not matching_table:
            response = self.database.create_table(
                AttributeDefinitions = [{
                    'AttributeName': EMAIL_COLUMN,
                    'AttributeType': 'S'
                }],
                KeySchema=[{
                    'AttributeName': EMAIL_COLUMN,
                    'KeyType': 'HASH'
                }],
                TableName = RECIPIENTS_TABLE
            )
        self.table = self.database.Table('recipients')

    def get_recipient(self, email):
        return self.table.query(KeyConditionExpression=Key(EMAIL_COLUMN).eq(email), ProjectionExpression=EMAIL_COLUMN, Limit=1)['Items']
    
    def all_recipients(self):
        return self.table.scan(ProjectionExpression=EMAIL_COLUMN)['Items']

    def find_recipients(self, email):
        return self.table.query(KeyConditionExpression=Key(EMAIL_COLUMN).contains(email), ProjectionExpression=EMAIL_COLUMN)['Items']
    
    def add_recipient(self, email):
        if not self.get_recipient(email):
            return self.table.put_item(
                Item={
                    EMAIL_COLUMN: email
                }
            )
        return False
    
    def delete_recipient(self, email):
        return self.table.remove_item(
            KeyConditionExpression=Key(EMAIL_COLUMN).eq(email), ProjectionExpression=EMAIL_COLUMN
        )