import json
import logging
import boto3
import cfnresponse
import time


ec2_client = boto3.client('ec2')
logs_client = boto3.client('logs')


def boto_throttle_backoff(boto_method, max_retries=10, backoff_multiplier=2, **kwargs):
    retry = 0
    results = None
    while not results:
        try:
            results = boto_method(**kwargs)
        except Exception as e:
            if 'ThrottlingException' in str(e) or 'VolumeInUse' in str(e):
                retry += 1
                if retry > max_retries:
                    print(f"Maximum retries of {str(max_retries)} reached")
                    raise
                print(
                    f"hit an api throttle, or eventual consistency error, waiting for {str(retry * backoff_multiplier)} seconds before retrying"
                )

                time.sleep(retry * backoff_multiplier)
            else:
                raise
    return results


def handler(event, context):
    print(f'Received event: {json.dumps(event)}')
    status = cfnresponse.SUCCESS
    physical_resource_id = 'PVCleanup'
    data = {}
    reason = None
    try:
        if event['RequestType'] == 'Delete':
            print('Removing any orphaned EBS volumes...')
            tag_name = f"tag:kubernetes.io/cluster/{event['ResourceProperties']['ClusterId']}"

            response = boto_throttle_backoff(
                ec2_client.describe_volumes,
                Filters=[{'Name': tag_name, 'Values': ['owned']}]
            )['Volumes']
            for volume in response:
                print(f"deleting volume {volume['VolumeId']}")
                boto_throttle_backoff(ec2_client.delete_volume, VolumeId=volume['VolumeId'])
    except Exception as e:
        logging.error(f'Exception: {e}', exc_info=True)
        reason = str(e)
        status = cfnresponse.FAILED
    finally:
        if event['RequestType'] == 'Delete':
            try:
                wait_message = f'waiting for events for request_id {context.aws_request_id} to propagate to cloudwatch...'

                while not logs_client.filter_log_events(
                        logGroupName=context.log_group_name,
                        logStreamNames=[context.log_stream_name],
                        filterPattern='"%s"' % wait_message
                )['events']:
                    print(wait_message)
                    time.sleep(5)
            except Exception as e:
                logging.error(f'Exception: {e}', exc_info=True)
                time.sleep(120)
        cfnresponse.send(event, context, status, data, physical_resource_id, reason)
