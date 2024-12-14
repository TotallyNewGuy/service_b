import os, json
from google.cloud import pubsub_v1
from google.oauth2 import service_account

from db_service import update_arrt_headway


publisher, subscriber = None, None


def init_pub_sub_client():
    global publisher, subscriber

    credentials_json = os.getenv("GCP_CRED")
    if credentials_json is None:
        raise ValueError("Didn't set GCP_CRED")
    credentials_info = json.loads(credentials_json)
    cred = service_account.Credentials.from_service_account_info(credentials_info)
    publisher = pubsub_v1.PublisherClient(credentials=cred)
    subscriber = pubsub_v1.SubscriberClient(credentials=cred)
    return publisher, subscriber


def get_publisher():
    return publisher


async def publish_message(json_data, topic):
    json_str = json.dumps(json_data)
    # Data must be a bytestring
    data = json_str.encode("utf-8")
    topic_path = get_publisher().topic_path(os.getenv("PROJECT_ID"), topic)
    # When you publish a message, the client returns a future.
    future = get_publisher().publish(topic_path, data)
    # print(f"Published {json_data} to topic {topic}, message_id: {future.result()}")


def get_subscriber():
    return subscriber


def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    if (message.data is not None):
        json_str = message.data.decode("utf-8")
        # filter out json string
        if json_str.startswith("{"):
            json_obj = json.loads(json_str)
            print(f"receive {json_obj} forom topic {os.getenv('TOPIC_ID')}")
            res_list = json_obj["respon_json_raw"]
            update_arrt_headway(res_list)
    message.ack()


def get_streaming_pull(curr_subscriber):
    subscription_path = curr_subscriber.subscription_path(os.getenv("PROJECT_ID"), os.getenv("SUB_ID"))
    streaming_pull_future = curr_subscriber.subscribe(subscription_path, callback=callback)
    return streaming_pull_future