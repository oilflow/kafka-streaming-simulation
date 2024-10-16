import hashlib
import json
import os
import random
import time
from datetime import datetime
from dotenv import load_dotenv
from kafka_utils import kafka_producer_config, kafka_consumer_config, delivery_report
from multiprocessing import Process
from postgre_func import connect_postgres


REGISTRATION_TOPIC = "user_registrations_topic"
LOGIN_TOPIC = "user_login_topic"
GROUP_ID = "user-login-group"

POSTGRES_HOST = 'localhost'
POSTGRES_PORT = '5432'
# POSTGRES_USER = os.getenv('POSTGRES_USER_FOR_MAIN_DB')
# POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD_FOR_MAIN_DB')
# POSTGRES_DB = os.getenv('POSTGRES_MAIN_DB')

STREAMING_DURATION = 24 * 60 * 60
# Pause time settings for day and nighttime
DAYTIME_MIN_PAUSE = 2  # minimum pause time during the day
DAYTIME_MAX_PAUSE = 30  # maximum pause time during the day
NIGHTTIME_MIN_PAUSE = 40  # minimum pause time at night
NIGHTTIME_MAX_PAUSE = 150  # maximum pause time at night


def generate_session_id():
    return 'SESSION-' + hashlib.sha256(datetime.now().isoformat().encode()).hexdigest()[-6:]


def generate_device_info():
    os_browser_mapping = {"Windows": ["Chrome", "Firefox", "Safari", "Edge", "Opera"],
                          "macOS": ["Chrome", "Firefox", "Safari", "Opera"],
                          "Linux": ["Chrome", "Firefox", "Opera"],
                          "Android": ["Chrome", "Firefox", "Opera"],
                          "iOS": ["Chrome", "Safari"]}

    os_list = list(os_browser_mapping.keys())
    op_sys = random.choice(os_list)
    browser = random.choice(os_browser_mapping[op_sys])

    start = list(map(int, '103.18.0.0'.split('.')))
    end = list(map(int, '192.224.241.255'.split('.')))
    ip_address = '.'.join(str(random.randint(start[i], end[i])) for i in range(4))

    return {
        "os": op_sys,
        "browser": browser,
        "ip_address": ip_address
    }


def get_user_ids_from_db(number_of_users):
    conn = connect_postgres(POSTGRES_HOST,
                            POSTGRES_PORT,
                            os.getenv('POSTGRES_USER_FOR_MAIN_DB'),
                            os.getenv('POSTGRES_PASSWORD_FOR_MAIN_DB'),
                            os.getenv('POSTGRES_MAIN_DB'))
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(id) FROM users WHERE registered <= NOW() - INTERVAL '20 min';")
    max_id = cursor.fetchone()[0]
    random_ids = [random.randint(1, max_id) for _ in range(number_of_users)]
    cursor.execute(f"SELECT user_id FROM users WHERE id IN ({','.join(str(i) for i in random_ids)})")
    user_ids = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return user_ids


def create_login_message(producer, user_id, topic):
    login_message = {
        'event_type': 'login',
        'user_id': user_id,
        'session_id': generate_session_id(),
        'timestamp': datetime.now().isoformat(),
        'device_info': generate_device_info()
    }
    producer.produce(topic, json.dumps(login_message), callback=delivery_report)
    producer.flush()


def process_user_registration_message(producer, message, topic):
    user_data = json.loads(message.value().decode('utf-8'))
    create_login_message(producer, user_data['user_id'], topic)


def login_producer_user_registration_task(consumer_topic, producer_topic):
    load_dotenv()
    us_reg_consumer = kafka_consumer_config(os.getenv('KAFKA_BOOTSTRAP_SERVERS'), GROUP_ID, 'earliest')
    login_producer = kafka_producer_config(os.getenv('KAFKA_BOOTSTRAP_SERVERS'))
    us_reg_consumer.subscribe([consumer_topic])
    while True:
        us_msg = us_reg_consumer.poll(timeout=1.0)
        if us_msg is None:
            continue
        if us_msg.error():
            print(f"Consumer error: {us_msg.error()}")
            continue
        process_user_registration_message(login_producer, us_msg, producer_topic)
        print(f"Registered {json.loads(us_msg.value().decode('utf-8')).get('user_id')} login data streamed at {time.strftime('%H:%M:%S')}")


def generate_logins_task():
    load_dotenv()
    producer = kafka_producer_config(os.getenv('KAFKA_BOOTSTRAP_SERVERS'))

    remaining_time = STREAMING_DURATION
    while remaining_time > 0:
        current_hour = time.localtime().tm_hour
        # day/night checking from condition 8 <= current_hour < 22
        if 8 <= current_hour < 22:
            pause_interval = random.randint(DAYTIME_MIN_PAUSE, DAYTIME_MAX_PAUSE)
        else:
            pause_interval = random.randint(NIGHTTIME_MIN_PAUSE, NIGHTTIME_MAX_PAUSE)
        users_id = get_user_ids_from_db(number_of_users=random.randint(1, 10))
        # print(users_id)
        for u_id in users_id:
            create_login_message(producer, u_id, LOGIN_TOPIC)
            print(f"Existing {u_id} login data streamed at {time.strftime('%H:%M:%S')} with pause {pause_interval} seconds.")
        time.sleep(pause_interval)
        remaining_time -= pause_interval


if __name__ == "__main__":
    reg_login_proc = Process(target=login_producer_user_registration_task, args=(REGISTRATION_TOPIC, LOGIN_TOPIC))
    gen_login_proc = Process(target=generate_logins_task)

    reg_login_proc.start()
    gen_login_proc.start()
