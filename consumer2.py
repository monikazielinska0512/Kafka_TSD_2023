import sys
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING


class Student:
    def __init__(self, name, marks):
        self.name = name
        self.marks = marks


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

    def reset_offset(consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)


    # Subscribe to topic
    topic = "purchases"
    consumer.subscribe([topic], on_assign=reset_offset)

    # GPA calculation
    students = []

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                print("Waiting...")
            elif msg.error():
                print("ERROR: {}".format(msg.error()))
            else:
                key = msg.key().decode('utf-8')
                value = msg.value().decode('utf-8')
                if key == 'student':
                    student_data = value.split(',')
                    name = student_data[0]
                    marks = [int(x) for x in student_data[1:]]
                    student = Student(name, marks)
                    students.append(student)

                    total_marks = sum(student.marks)
                    gpa = total_marks / len(student.marks)

                    print("Consumer 2 - Student: {}, GPA: {:.2f}".format(student.name, gpa))
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()


