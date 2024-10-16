import json
import os
import requests
import random
import time
from datetime import datetime
from dotenv import load_dotenv
from kafka_utils import kafka_producer_config, delivery_report


ENDPOINT = "https://randomuser.me/api/?results=1&nat=dk,fr,gb,ch,de,nl,no,es,ie,fi,rs"
# KAFKA_BOOTSTRAP_SERVERS = ['kafka_broker_1:9092', 'kafka_broker_2:19093', 'kafka_broker_3:19094']  # if script is inside conteiner
# KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092', 'localhost:9093', 'localhost:9094']
KAFKA_TOPIC = "user_registrations_topic"

STREAMING_DURATION = 24 * 60 * 60
# Pause time settings for day and nighttime
DAYTIME_MIN_PAUSE = 50     # minimum pause time during the day
DAYTIME_MAX_PAUSE = 160    # maximum pause time during the day
NIGHTTIME_MIN_PAUSE = 161  # minimum pause time at night
NIGHTTIME_MAX_PAUSE = 280  # maximum pause time at night


def get_random_users(url=ENDPOINT) -> dict:
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()['results'][0]
        data['registered'] = datetime.now().isoformat()
        return data
    else:
        raise Exception(f"API request failed with status code {response.status_code}")


def publish_user_registration(producer, user_data, topic):
    message = {'event_type': 'registration',
               'user_id': user_data['login']['uuid'],
               "timestamp": datetime.now().isoformat(),
               "user_data": {'first_name': user_data['name']['first'],
                             'last_name': user_data['name']['last'],
                             'gender': user_data['gender'],
                             'date_of_birth': user_data['dob']['date'].split('T')[0],
                             'phone': user_data['cell'],
                             'country': user_data['location']['country'],
                             'state': user_data['location']['state'],
                             'city': user_data['location']['city'],
                             'street': user_data['location']['street']['name'],
                             'number': user_data['location']['street']['number'],
                             'postcode': user_data['location']['postcode'],
                             'coordinates': user_data['location']['coordinates'],
                             'email': user_data['email'],
                             'username': user_data['login']['username'],
                             'password': user_data['login']['password'],
                             'registered': user_data['registered'],
                             'nat': user_data['nat']}}
    producer.produce(topic, json.dumps(message), callback=delivery_report)
    producer.flush()


if __name__ == "__main__":
    load_dotenv()
    reg_producer = kafka_producer_config(os.getenv('KAFKA_BOOTSTRAP_SERVERS'))
    remaining_time = STREAMING_DURATION
    while remaining_time > 0:
        current_hour = time.localtime().tm_hour
        # day/night checking from condition 8 <= current_hour < 22
        if 8 <= current_hour < 22:
            pause_interval = random.randint(DAYTIME_MIN_PAUSE, DAYTIME_MAX_PAUSE)
        else:
            pause_interval = random.randint(NIGHTTIME_MIN_PAUSE, NIGHTTIME_MAX_PAUSE)
        user_full = get_random_users()
        publish_user_registration(reg_producer, user_full, KAFKA_TOPIC)
        print(f"Data streamed at {time.strftime('%H:%M:%S')} with pause {pause_interval} seconds.")
        time.sleep(pause_interval)
        remaining_time -= pause_interval
