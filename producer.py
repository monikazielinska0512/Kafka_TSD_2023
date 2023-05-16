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
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    # Parse the configuration.
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    # Create Producer instance
    producer = Producer(config)


    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
            # Generate random student data

    topic = "grades"
    students = [
        Student("Alice", [90, 85, 95]),
        Student("Bob", [80, 75, 70]),
        Student("Charlie", [95, 90, 85])
    ]

    # Produce data for each student
    for student in students:
        key = student.name
        print(student.name)
        value = ','.join(map(str, student.marks))
        producer.produce(topic, key=key, value=value, callback=delivery_callback)

    # Block until the messages are sent.
    producer.flush()
