import boto3
import os
from datetime import datetime, timezone, timedelta
import json
import requests

dynamodb = boto3.client('dynamodb')

def create_backup(table_name,backup_name, slack_webhook):
    try:
        response = dynamodb.create_backup(
            TableName=table_name,
            BackupName=backup_name
        )
        backup_arn = response['BackupDetails']['BackupArn']
        print(f"✅ Created backup: {backup_name}")
        if slack_webhook:
            message = {
                "blocks": [
                    {"type": "header", "text": {"type": "plain_text", "text": "📦 DynamoDB Backup Summary"}},
                    {"type": "section", "fields": [
                        {"type": "mrkdwn", "text": f"*Table:* {table_name}"},
                        {"type": "mrkdwn", "text": f"*New Backup:* {backup_name}"},
                        {"type": "mrkdwn", "text": f"*Timestamp:* {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}"}
                    ]}
                ]
            }
        requests.post(slack_webhook, data=json.dumps(message))
        return {"status": "success", "created_backup": backup_name}
    except Exception as e:
        print(f"❌ Error during backup operation: {str(e)}")
        if slack_webhook:
            error_msg = {"text": f":x: *DynamoDB Backup Failed* for `{table_name}`.\nError: {str(e)}"}
            requests.post(slack_webhook, data=json.dumps(error_msg))
        raise e

def delete_old_backup(table_name,backup_name, slack_webhook):
    try:
        retention_days = os.environ.get('RETENTION_DAYS') 
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=int(retention_days))
        backups = dynamodb.list_backups(TableName=table_name)['BackupSummaries']
        deleted_count = 0
        for backup in backups:
            if 'BackupCreationDateTime' in backup and backup['BackupCreationDateTime'] < cutoff_date:
                dynamodb.delete_backup(BackupArn=backup['BackupArn'])
                deleted_count += 1
                print(f"🗑️ Deleted old backup: {backup['BackupName']}")
        if slack_webhook:
            message = {
                "blocks": [
                    {"type": "header", "text": {"type": "plain_text", "text": "📦 DynamoDB Deleted Old Backups Summary"}},
                    {"type": "section", "fields": [
                        {"type": "mrkdwn", "text": f"*Table:* {table_name}"},
                        {"type": "mrkdwn", "text": f"*Deleted Old Backups:* {deleted_count}"},
                        {"type": "mrkdwn", "text": f"*Timestamp:* {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}"}
                    ]}
                ]
            }
        requests.post(slack_webhook, data=json.dumps(message))
        return {"status": "success", "deleted_backups": deleted_count}
    except Exception as e:
        print(f"❌ Error during backup operation: {str(e)}")
        if slack_webhook:
            error_msg = {"text": f":x: *DynamoDB Delete Old Backup Failed* for `{table_name}`.\nError: {str(e)}"}
            requests.post(slack_webhook, data=json.dumps(error_msg))
        raise e

def lambda_handler(event, context):
    table_name = os.environ['TABLES_NAME']
    slack_webhook = os.environ.get('SLACK_WEBHOOK_URL')
    table_name_list = [t.strip() for t in table_name.split(",")]
    timestamp = datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S")
    try:
        for i in table_name_list:
            backup_name = f"{i}-backup-{timestamp}"
            create_backup(i,backup_name,slack_webhook)
        for i in table_name_list:
            backup_name = f"{i}-backup-{timestamp}"
            delete_old_backup(i,backup_name,slack_webhook)
    except Exception as e:
        print(f"❌ Error during backup operation: {str(e)}")
        if slack_webhook:
            error_msg = {"text": f":x: *DynamoDB Backup Failed*`.\nError: {str(e)}"}
            requests.post(slack_webhook, data=json.dumps(error_msg))
        raise e