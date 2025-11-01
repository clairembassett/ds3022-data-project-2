import requests
import boto3
import time
from prefect import flow, task, get_run_logger

sqs = boto3.client("sqs")

@task 
# Getting the Queue URL by sending a post request to the API with approved credentialsConnecting to the API by sending a post request to recieve the Queue Url
def api_request():
    logger = get_run_logger()
    url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/qxm6fm"

    try:
        logger.info("Starting POST request to API")
        payload = requests.post(url).json()
        queue_url = payload.get("sqs_url")
        logger.info(f"Queue URL received: {queue_url}")
        return queue_url
    
    except Exception as e:
        logger.error(f"Failed to receive URL: {e}", exc_info=True) 


#Getting queue attributes to track the recieving of messages
def get_queue_attributes(queue_url):
    logger = get_run_logger()
    try:
        response = sqs.get_queue_attributes(
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
        logger.info(f'Queue Status - Available: {available}, Not Visible: {not_visible}, Delayed: {delayed}, Total: {total}')
        return available, not_visible, delayed, total

    except Exception as e:
        logger.error(f"Error getting queue attributes: {e}")
        raise


@task
def recieve_and_parse(queue_url):
    logger = get_run_logger()
    puzzle_pieces = {}
    receipts = []

    logger.info("Starting to receive and parse messages")

    while len(puzzle_pieces) < 21:
        available, not_visible, delayed, total = get_queue_attributes(queue_url)
        logger.info(f"Queue Status - Available: {available}, Not Visible: {not_visible}, Delayed: {delayed}, Total: {total}")

        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=7,
            MessageAttributeNames=['All'],
            WaitTimeSeconds=2
        )

        messages = response.get("Messages", [])

        if not messages:
            logger.info(f"No additional messages available: {len(puzzle_pieces)}/21 pieces collected")
            time.sleep(30)
            continue

        for msg in messages:
            num = int(msg["MessageAttributes"]["order_no"]["StringValue"])
            word = msg["MessageAttributes"]["word"]["StringValue"]
            receipt = msg["ReceiptHandle"]
            receipts.append(receipt)

            if num not in puzzle_pieces:
                puzzle_pieces[num] = word
                logger.info(f"Collected piece {num}: '{word}' (total: {len(puzzle_pieces)}/21)")
            else:
                logger.info(f"Duplicate piece {num} skipped")
        
        time.sleep(30)

    logger.info("All 21 pieces collected and parsed successfully!")
    puzzle_pieces_list = [(num, word) for num, word in puzzle_pieces.items()]
    return {"Puzzle Pieces": puzzle_pieces_list, "Receipts": receipts}

# Deleting messages
@task
def delete_messages(queue_url, receipts):
    logger = get_run_logger()
    logger.info(f"Deleting all messages")

    deleted = 0
    for handle in receipts:
        try:
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=handle)
            deleted += 1
                
        except Exception as e:
            logger.error(f"Failed to delete message: {e}", exc_info=True)

    logger.info(f"Deleted {deleted}/{len(receipts)}")

# Decoding Message
@task
def decode_message(pieces):
    logger = get_run_logger()
    decoded = sorted(pieces, key=lambda x: x[0])
    message = " ".join(word for _, word in decoded)
    logger.info(f"Decoded message: '{message}'")
    return message

# Sending Solution
@task
def send_solution(id, message, platform):
    logger = get_run_logger()
    mailbox_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"

    try:
        response = sqs.send_message(
            QueueUrl = mailbox_url,
            MessageBody = message,
            MessageAttributes = {
                'uvaid': {'DataType': 'String', 'StringValue': 'qxm6fm'},
                'phrase': {'DataType': 'String', 'StringValue': message},
                'platform': {'DataType': 'String', 'StringValue': platform}
            }
        )
        logger.info(f"Solution sent successfully: {response}")

    except Exception as e:
        logger.error(f"Failed to send solution: {e}", exc_info=True)

# Orchestrate all tasks
@flow(log_prints=True)
def dp2_pipeline(platform='prefect'):
    logger = get_run_logger()
    logger.info(f"Starting DP2 pipeline with platform={platform}")

    queue_url = api_request()

    # Wait for messages

    available, not_visible, delayed, total = get_queue_attributes(queue_url)


    # Receive and parse
    result = recieve_and_parse(queue_url)
    receipts = result["Receipts"]
    puzzle_pieces = result["Puzzle Pieces"]

    # Delete messages
    delete_messages(queue_url, receipts)

    # Decode and send solution
    message = decode_message(puzzle_pieces)
    send_solution("qxm6fm", message, platform)

    logger.info("Pipeline completed successfully")
    return message

if __name__ == "__main__":
    dp2_pipeline()







    













            
















