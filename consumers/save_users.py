import json
import os
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
from postgre_func import connect_postgres, create_table, insert_user


load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_GROUP_ID = 'user-registration-group'
KAFKA_TOPIC = 'user_registrations_topic'

POSTGRES_HOST = 'localhost'
POSTGRES_PORT = '5432'
POSTGRES_USER = os.getenv('POSTGRES_USER_FOR_MAIN_DB')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD_FOR_MAIN_DB')
POSTGRES_DB = os.getenv('POSTGRES_MAIN_DB')

create_users = """CREATE TABLE IF NOT EXISTS users (
                        id SERIAL PRIMARY KEY,
                        first_name VARCHAR(100),
                        last_name VARCHAR(100),
                        gender VARCHAR(10),
                        date_of_birth DATE,
                        phone VARCHAR(20),
                        country VARCHAR(100),
                        city VARCHAR(100),
                        email VARCHAR(100) UNIQUE,
                        user_id UUID UNIQUE,
                        username VARCHAR(100),
                        password VARCHAR(255),
                        registered TIMESTAMP,
                        nat VARCHAR(10));"""


insert_to_users = """INSERT INTO users (
                        first_name, last_name, gender, date_of_birth, phone, country, city,
                        email, user_id, username, password, registered, nat
                    ) VALUES (
                        %(first_name)s, %(last_name)s, %(gender)s, %(date_of_birth)s, %(phone)s, %(country)s,
                        %(city)s, %(email)s, %(user_id)s, %(username)s, %(password)s, %(registered)s, %(nat)s
                    )
                    ON CONFLICT (email) DO NOTHING;"""


def user_data_cleaning(user_data: dict) -> dict:
    data = {
        'first_name': user_data['user_data']['first_name'],
        'last_name': user_data['user_data']['last_name'],
        'gender': user_data['user_data']['gender'],
        'date_of_birth': user_data['user_data']['date_of_birth'],
        'phone': user_data['user_data']['phone'],
        'country': user_data['user_data']['country'],
        'city': user_data['user_data']['city'],
        'email': user_data['user_data']['email'],
        'user_id': user_data['user_id'],
        'username': user_data['user_data']['username'],
        'password': user_data['user_data']['password'],
        'registered': user_data['user_data']['registered'],
        'nat': user_data['user_data']['nat'],
    }
    return data


def consume_messages(conn_db, kafka_servers, kafka_group_id, kafka_offset_reset, kafka_topic, sql_query):
    consumer = Consumer({'bootstrap.servers': kafka_servers,
                         'group.id': kafka_group_id,
                         'auto.offset.reset': kafka_offset_reset})
    consumer.subscribe([kafka_topic])
    try:
        while True:
            message = consumer.poll(timeout=1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Consumer error: {message.error()}")
                    continue
            # transform
            user_data = user_data_cleaning(json.loads(message.value().decode('utf-8')))
            # save
            insert_user(user_data, conn_db, sql_query)
    except Exception as msg:
        print(f"Error while consuming messages: {msg}")
    finally:
        consumer.close()


if __name__ == "__main__":
    conn = connect_postgres(POSTGRES_HOST,
                            POSTGRES_PORT,
                            POSTGRES_USER,
                            POSTGRES_PASSWORD,
                            POSTGRES_DB)
    create_table(conn, create_users)
    consume_messages(conn,
                     KAFKA_BOOTSTRAP_SERVERS,
                     KAFKA_GROUP_ID,
                     'earliest',
                     KAFKA_TOPIC,
                     insert_to_users)
    conn.close()
