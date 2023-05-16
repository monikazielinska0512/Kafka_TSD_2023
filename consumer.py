#!/usr/bin/env python

import sys
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING


class Student:
    def __init__(self, name, marks):
        self.name = name
        self.marks = marks

    def calculate_gpa(self):
        total_marks = sum(self.marks)
        gpa = total_marks / len(self.marks)
        return gpa


if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('--reset', action='store_true')
    args = parser.parse_args()

    # Parse the configuration.
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])
    config.update(config_parser['consumer'])

    # Create Consumer instance
    consumer = Consumer(config)


    # Set up a callback to handle the '--reset' flag.
    def reset_offset(consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)


    # Subscribe to topic
    topic = "grades"
    consumer.subscribe([topic], on_assign=reset_offset)

    # Initialize list to store students
    students = []

    # Poll for new messages from Kafka and calculate GPA.
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting...")
            elif msg.error():
                print("ERROR: {}".format(msg.error()))
            else:
                # Extract the (optional) key and value.
                name, marks = msg.key().decode('utf-8'), msg.value().decode('utf-8')
                marks = list(map(int, marks.split(',')))

                # Create Student instance
                student = Student(name, marks)
                students.append(student)

                # Calculate GPA
                gpa = student.calculate_gpa()
                print("Student: {name}, GPA: {gpa}".format(name=name, gpa=gpa))

    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()

