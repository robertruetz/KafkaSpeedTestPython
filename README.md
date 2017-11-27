# KafkaSpeedTestPython
Python 2.7.11 project that attempts to measure kafka throughput speed by producing/consuming n number of messages.

Must set environment var: ```KAFKA_BROKERS=100.0.0.0:10080```

USAGE:

```python kafka.py numOfMessages``` : Produce and consume numOfMessages synchronously

```python kafka_mt.py numOfMessages``` : Produce and consume numOfMessages asynchronously
