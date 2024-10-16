from confluent_kafka import Producer, Consumer


def kafka_producer_config(servers):
    return Producer({'bootstrap.servers': servers})


def kafka_consumer_config(servers, group, offset):
    return Consumer({'bootstrap.servers': servers,
                     'group.id': group,
                     'auto.offset.reset': offset})


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        # print(f"Message delivered to {msg.topic()} [{msg.partition()}]")
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")
