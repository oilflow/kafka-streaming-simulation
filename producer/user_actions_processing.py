import json
import os
import random
from dotenv import load_dotenv
from kafka_utils import kafka_producer_config, kafka_consumer_config, delivery_report
from session_generation import simulate_user_session


LOGIN_TOPIC = "user_login_topic"
USER_ACTIONS_TOPIC = "user_actions_topic"
GROUP_ID = "user-actions-group"


def process_login_message(producer, message, producer_topic):
    login_data = json.loads(message.value().decode('utf-8'))

    num_interacted_pages = random.randint(5, 15)
    num_actions = random.randint(3, 15)

    session_data = simulate_user_session(num_interacted_pages=num_interacted_pages,
                                         num_actions=num_actions,
                                         session_id=login_data['session_id'],
                                         user_id=login_data['user_id'])
    producer.produce(producer_topic, session_data, callback=delivery_report)
    producer.flush()


def user_actions_processing_task(consumer_topic, producer_topic):
    load_dotenv()
    consumer = kafka_consumer_config(os.getenv('KAFKA_BOOTSTRAP_SERVERS'), GROUP_ID, 'earliest')
    producer = kafka_producer_config(os.getenv('KAFKA_BOOTSTRAP_SERVERS'))
    consumer.subscribe([consumer_topic])

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue
        process_login_message(producer, msg, producer_topic)
        print(f"Session data for user_id: {json.loads(msg.value().decode('utf-8')).get('user_id')} with session_id {json.loads(msg.value().decode('utf-8')).get('session_id')}")


if __name__ == "__main__":
    user_actions_processing_task(LOGIN_TOPIC, USER_ACTIONS_TOPIC)

