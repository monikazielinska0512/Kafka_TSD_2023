#!/usr/bin/env python

import sys
from random import choice
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer

class Student:
    def __init__(self, name, marks):
        self.name = name
        self.marks = marks

if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file',
    type=FileType('r'))
    args = parser.parse_args()

    # Parse the configuration.
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    # Create Producer instance
    producer = Producer(config)


    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))


    # Produce data by selecting random values from these lists.
    topic = "purchases"
    students = [
        Student('John', [85, 90, 92]),
        Student('Emma', [78, 82, 80]),
        Student('Michael', [92, 88, 90]),
        Student('Sophia', [95, 92, 96]),
        Student('William', [80, 85, 82])
    ]

    for student in students:
        message_key = 'student'
        message_value = "{},{}".format(student.name, ','.join(str(x) for x in student.marks))
        producer.produce(topic, key=message_key, value=message_value, callback=delivery_callback)

    # Block until the messages are sent.
    producer.poll(10000)
    producer.flush()
