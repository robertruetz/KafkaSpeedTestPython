import confluent_kafka as kafka
from confluent_kafka import Consumer, Producer, KafkaException, KafkaError
import avro.schema
import avro.datafile
import io
import random
import instrumentation as inst
from instrumentation import Timer
import sys
import uuid
import os


BROKERS = os.environ.get("KAFKA_BROKERS", None)
ERRORS = 0
DELIVERED = 0


def main(args):
    if BROKERS is None:
        print("KAFKA_BROKERS env var missing.")
        sys.exit(2)
    config = {"bootstrap.servers": BROKERS}
    topic = "speed_test"

    schema_path = r"./user.avsc"
    schema = avro.schema.parse(open(schema_path).read())

    sw = Timer()
    number = 1000
    if len(args) > 0:
        number = int(args[0])

    #create messages
    inst.GLOBAL_TIMER.start()
    print("Creating {num} messages".format(num=number))
    sw.start()
    msgs = create_random_payloads(number, schema)
    sw.stop()
    print("Message creation completed in: {time} ms".format(time=sw.to_string()))

    #produce messages
    print("Producing messages to Kafka")
    sw.start()
    produce_messages(msgs, topic, config)
    sw.stop()
    print("{msgs} messages produced in: {time} ms".format(msgs=DELIVERED, time=sw.to_string()))

    #consume messages
    print("Consuming messages from Kafka")
    sw.start()
    consumer_config = {'bootstrap.servers': BROKERS,
                       'group.id': str(uuid.uuid4()),
                       'default.topic.config': {'auto.offset.reset': 'smallest'}}
    received = consume_messages(consumer_config, topic)
    sw.stop()
    print("{num} messages received in {time} ms".format(num=len(received), time=sw.to_string()))

    # deserialize received messages
    print("Deserializing messages.")
    sw.start()
    deserialized = []
    for msg in received:
        deserialized.append(avro_deserialize_payload(msg, schema))
    sw.stop()
    print("{num} messages deserialized in {ms} ms".format(num=len(deserialized), ms=sw.to_string()))

    inst.GLOBAL_TIMER.stop()
    print("Entire script took {ms} ms\n".format(ms=inst.GLOBAL_TIMER.to_string()))


def consume_messages(consumer_config, topic):
    received = []
    consumer = Consumer(**consumer_config)
    consumer.subscribe([topic], on_assign=on_assign)
    while len(received) < DELIVERED and ERRORS == 0:
        msg = consumer.poll(timeout=0.5)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print("End of partition reached. Breaking operation.")
                break
            else:
                raise KafkaException(msg.error())
        else:
            received.append(msg)
    return received


def consume_messages_ref(consumer_config, topic, message_list):
    message_list.extend(consume_messages(consumer_config, topic))


def produce_messages(msgs, topic, config):
    prod = Producer(**config)
    for msg in msgs:
        prod.produce(topic, msg, callback=delivery_callback)
        prod.poll(0)
    prod.flush()


def on_assign(consumer, partitions):
    print("Partitions assigned: {parts}".format(parts=partitions))


def delivery_callback(err, msg):
    global ERRORS
    global DELIVERED
    if err:
        ERRORS += 1
    else:
        DELIVERED += 1


def avro_deserialize_payload(payload, schema):
    bytes_reader = io.BytesIO(payload.value())
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)
    return reader.read(decoder)


def avro_serialize_payload(payload, schema):
    writer = avro.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write(payload, encoder)
    return bytes_writer.getvalue()


def create_random_payloads(number, schema):
    msgs = []
    for i in range(number):
        msg = avro_serialize_payload({"name": random_string(10), "favorite_color": random_color(), "favorite_number": random.randint(0, 100)}, schema)
        msgs.append(msg)
    return msgs


def random_string(length):
    return ''.join([random.choice("abcdefghijklmnopqrstuvwxyz") for x in range(length)])


def random_color():
    return random.choice(["red", "blue", "green", "yellow", "orange", "purple"])


if __name__ == "__main__":
    main(sys.argv[1:])
