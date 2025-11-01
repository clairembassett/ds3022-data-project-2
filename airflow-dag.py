import requests
import boto3
import time
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.sqs import SqsHook
from airflow.models import Variable
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

sqs = boto3.client("sqs", region_name="us-east-1")

@task
def api_request():
    # Getting the Queue URL by sending a post request to the API with approved credentials
    api_url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/qxm6fm"
    try:
        print(f"Sending POST request to {api_url}")
        response = requests.post(api_url)
        response.raise_for_status()
        queue_url = response.json()["sqs_url"]
        print(f"Extracted queue URL: {queue_url}")
        return queue_url
    except Exception as e:
        raise AirflowException(f"Failed to get queue URL: {e}")

@task 
def get_queue_count(queue_url):
    """Helper function to get queue attributes"""
    sqs_hook = SqsHook(aws_conn_id="aws_default", region_name="us-east-1")
    sqs_client = sqs_hook.get_conn()
    response = sqs_client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=[
            'ApproximateNumberOfMessages',
            'ApproximateNumberOfMessagesNotVisible',
            'ApproximateNumberOfMessagesDelayed'
        ]
    )

    attributes = response['Attributes']
    available = int(attributes.get('ApproximateNumberOfMessages', 0))
    not_visible = int(attributes.get('ApproximateNumberOfMessagesNotVisible', 0))
    delayed = int(attributes.get('ApproximateNumberOfMessagesDelayed', 0))
    total = available + not_visible + delayed

    print(f"Queue Status — Available={available}, NotVisible={not_visible}, Delayed={delayed}, Total={total}")
    return total


@task
#  Recieves messages from SQS, getting the message attributes, 
def recieve_and_parse(queue_url):
    print("Receiving all messages")
    sqs_hook = SqsHook(aws_conn_id='aws_default', region_name='us-east-1')
    sqs_client = sqs_hook.get_conn()

    puzzle_pieces = {}
    receipts = []

    print("Starting to recieve and parse messages")


    while len(puzzle_pieces) < 21:
        # Check queue status each iteration
        attrs = sqs_client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=[
                'ApproximateNumberOfMessages',
                'ApproximateNumberOfMessagesNotVisible',
                'ApproximateNumberOfMessagesDelayed'
            ]
        )['Attributes']

        available = int(attrs.get('ApproximateNumberOfMessages', 0))
        not_visible = int(attrs.get('ApproximateNumberOfMessagesNotVisible', 0))
        delayed = int(attrs.get('ApproximateNumberOfMessagesDelayed', 0))
        total = available + not_visible + delayed

        print(f"Queue Status — Available={available}, NotVisible={not_visible}, Delayed={delayed}, Total={total}")

        # Receive up to 7 messages at a time
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=7,
            MessageAttributeNames=['All'],
            WaitTimeSeconds=2
        )

        messages = response.get("Messages", [])

        if not messages:
            print(f"No messages received ({len(puzzle_pieces)}/21 pieces collected). Waiting 30s...")
            time.sleep(30)

        for msg in messages:
            num = int(msg["MessageAttributes"]["order_no"]["StringValue"])
            word = msg["MessageAttributes"]["word"]["StringValue"]
            receipt = msg["ReceiptHandle"]
            receipts.append(receipt)

            if num not in puzzle_pieces:
                puzzle_pieces[num] = word
                print(f"Collected piece {num}: '{word}' ({len(puzzle_pieces)}/21)")
            else:
                print(f"Duplicate piece {num} skipped")

    print(f"All 21 pieces collected and parsed successfully!")
    puzzle_pieces_list = [(num, word) for num, word in puzzle_pieces.items()]
    return {
            "puzzle_pieces": puzzle_pieces_list,
            "receipts": receipts
        }
    
# Deleting messages
@task
def delete_messages(queue_url, parsed_data):
    print(f"Deleting all messages")
    receipts = parsed_data["receipts"]
    sqs_hook = SqsHook(aws_conn_id='aws_default', region_name='us-east-1')
    sqs_client = sqs_hook.get_conn()
    deleted = 0

    for handle in receipts:
        try:
            sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=handle)
            deleted += 1
                
        except Exception as e:
            print(f"Failed to delete message: {e}", exc_info=True)

    print(f"Deleted {deleted}/{len(receipts)}")

# Decoding Message
@task
def decode_message(parsed_data):
    puzzle_pieces = parsed_data["puzzle_pieces"]
    print("Decoding message...")
    sorted_pieces = sorted(puzzle_pieces, key=lambda x: x[0])
    message = " ".join(word for _, word in sorted_pieces)
    print(f"Decoded message: {message}")
    return message

# Sending Solution
@task
def send_solution(message):
    print("Sending solution to SQS...")
    sqs_hook = SqsHook(aws_conn_id='aws_default', region_name='us-east-1')
    sqs_client = sqs_hook.get_conn()

    mailbox_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"

    try:
        response = sqs_client.send_message(
            QueueUrl = mailbox_url,
            MessageBody = message,
            MessageAttributes = {
                'uvaid': {'DataType': 'String', 'StringValue': 'qxm6fm'},
                'phrase': {'DataType': 'String', 'StringValue': message},
                'platform': {'DataType': 'String', 'StringValue': "airflow"}
            }
        )
        print(f"Solution sent successfully, MessageId = {response.get('MessageId')}")

    except Exception as e:
        print(f"Failed to send solution: {e}", exc_info=True)

@task
def verify_queue_empty(queue_url):
    sqs_hook = SqsHook(aws_conn_id='aws_default', region_name='us-east-1')
    sqs_client = sqs_hook.get_conn()
    
    response = sqs_client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=[
            'ApproximateNumberOfMessages',
            'ApproximateNumberOfMessagesNotVisible',
            'ApproximateNumberOfMessagesDelayed'
        ]
    )
    
    attributes = response['Attributes']
    available = int(attributes.get('ApproximateNumberOfMessages', 0))
    not_visible = int(attributes.get('ApproximateNumberOfMessagesNotVisible', 0))
    delayed = int(attributes.get('ApproximateNumberOfMessagesDelayed', 0))
    remaining = available + not_visible + delayed
    
    if remaining == 0:
        print("Queue is empty!")
    else:
        print(f"Queue still has {remaining} messages remaining.")
    return remaining


# Orchestrate all tasks
with DAG(
    "dp2_dag",
    default_args=default_args,
    description="DP2 Puzzle Pipeline with Separate Delete Step",
    schedule_interval=None,
    catchup=False,
    tags=["dp2", "sqs", "puzzle"],
) as dag:

    queue_url = api_request()
    queue_count = get_queue_count(queue_url)
    parsed = recieve_and_parse(queue_url)

    deleted = delete_messages(queue_url, parsed)
    decoded = decode_message(parsed)
    check = verify_queue_empty(queue_url)
    send = send_solution(decoded)

    queue_url >> queue_count >> parsed
    parsed >> [deleted, decoded]
    decoded >> [check, send]