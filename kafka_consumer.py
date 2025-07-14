from confluent_kafka import Consumer, KafkaException, KafkaError
import sys

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'kafka-broker-1:19092'
KAFKA_TOPIC = 'weather-readings-raw'

def consume_messages():
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'my_consumer_group',
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(conf)

    try:
        consumer.subscribe([KAFKA_TOPIC])

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' % (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # Proper message
                print(f"Received message: Key={msg.key().decode('utf-8') if msg.key() else 'None'}, Value={msg.value().decode('utf-8')}")

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')
    finally:
        consumer.close()

if __name__ == '__main__':
    consume_messages()

