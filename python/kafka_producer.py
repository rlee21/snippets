import io
import json
import time
from confluent_kafka import Producer
from fastavro import writer
from pyspark import SparkContext, HiveContext, SparkConf
from pyspark.sql import HiveContext


def get_sql():
    """ get sql query to be used for data extraction """
    sql = sqlContext.sql(""" SELECT
                                 uid,
                                 record_flag,
                                 professional_id,
                                 specialty_id,
                                 topic_id,
                                 skill_name,
                                 event_name,
                                 event_timestamp,
                                 event_app_id
                            FROM src.professional_skills_kafka """)
    return sql


def get_data(sql):
    """ get data using spark and return list """
    messages = []
    data = sql.collect()
    for row in data:
        event_data = {"name": row[6], "timestamp": row[7], "app_id": row[8]}
        messages.append({"uid": row[0],
                         "record_flag": row[1],
                         "professional_id": row[2],
                         "specialty_id": row[3],
                         "topic_id": row[4],
                         "skill_name": row[5],
                         "event": event_data})
    return messages


def get_message(messages):
    """ yields messages from data """
    for message in messages:
        yield message


def send(topic, raw_bytes):
    # TODO: add error handling and write to kafka 0.10
    """ Send encoded Avro message to Kafka """
    producer.produce(topic, raw_bytes)


def main(messages, schema, topic):
    """
    Encode each message to Avro and send to Kafka
    Calls get_message and send functions
    """
    for message in get_message(messages):
        bytes_writer = io.BytesIO()
        writer(bytes_writer, schema, [message])
        raw_bytes = bytes_writer.getvalue()
        send(topic, raw_bytes)


if __name__ == '__main__':
    start_time = time.time()
    conf = {
        'bootstrap.servers': 'kafka1test.prod.avvo.com:9092',
        'api.version.request': 'false',
        'broker.version.fallback': '0.8.0',
        'queue.buffering.max.messages': 10000000,
        'retries': 3,
        'delivery.report.only.error': True
    }
    producer = Producer(**conf)
    with open('skills_schema.avsc', 'r') as f:
        schema = json.loads(f.read())
    topic = 'skills'
    sc = SparkContext()
    sqlContext = HiveContext(sc)
    # function calls
    sql = get_sql()
    messages = get_data(sql)
    main(messages, schema, topic)
    producer.flush()

    print("completed in: {0} seconds".format(time.time() - start_time))
