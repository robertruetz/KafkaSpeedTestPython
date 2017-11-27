import gevent.monkey
gevent.monkey.patch_socket()

import gevent
import sys
import kafka
import avro.schema
import avro.datafile
from instrumentation import Timer
import instrumentation as inst
import uuid


def main(args):
    if kafka.BROKERS is None:
        print("KAFKA_BROKERS env var not set.")
        sys.exit(2)
    config = {"bootstrap.servers": kafka.BROKERS}
    topic = "speed_test"

    schema_path = r"./user.avsc"
    schema = avro.schema.parse(open(schema_path).read())

    sw = Timer()
    number = 1000
    if len(args) > 0:
        number = int(args[0])

    # create messages
    inst.GLOBAL_TIMER.start()
    print("Creating {num} messages".format(num=number))
    sw.start()
    msgs = kafka.create_random_payloads(number, schema)
    sw.stop()
    print("Message creation completed in: {time} ms".format(time=sw.to_string()))

    consumer_config = {'bootstrap.servers': kafka.BROKERS,
                       'group.id': str(uuid.uuid4()),
                       'default.topic.config': {'auto.offset.reset': 'smallest'}}

    # produce and consume messages async
    threads = []
    received = []
    sw.start()
    threads.append(gevent.spawn(kafka.produce_messages, msgs, topic, config))
    threads.append(gevent.spawn(kafka.consume_messages_ref, consumer_config, topic, received))
    gevent.joinall(threads)
    sw.stop()
    print("{sent} messages sent and {rec} messages received in {ms} ms".format(sent=len(msgs), rec=len(received), ms=sw.to_string()))

    # deserialize messages
    deserialized = []
    sw.start()
    for msg in received:
        deserialized.append(kafka.avro_deserialize_payload(msg, schema))
    sw.stop()
    print("{num} messages deserialized in {ms} ms.".format(num=len(deserialized), ms=sw.to_string()))
    inst.GLOBAL_TIMER.stop()
    print("Entire script took {ms} ms.".format(ms=inst.GLOBAL_TIMER.to_string()))


if __name__ == "__main__":
    main(sys.argv[1:])
