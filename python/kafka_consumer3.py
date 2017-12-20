import sys
import io

import avro.schema

from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

from kafka import KafkaConsumer
from kafka import TopicPartition

# To consume latest messages and auto-commit offsets
#p
# To consume messages from a specific PARTITION  [ FIX ]

topic = 'SubscriptionSaved'
# topic = sys.argv[1]
partition = 0
# partition = int(sys.argv[2])
offset = 58119
# offset = int(sys.argv[3])

kafkaBroker = 'kafka1wow.prod.avvo.com:9092'
if len(sys.argv) > 4:
    kafkaBroker = sys.argv[4]
print ('Using kafka broker: %s' % kafkaBroker)

topicPartition = TopicPartition(topic, partition)
print ('Starting consumer for topic [%s] from partition %d' % (topic, partition))
consumer = KafkaConsumer(bootstrap_servers=kafkaBroker)
consumer.assign([topicPartition])

print ('Moving partition %d offset to %d' % (partition, offset))
consumer.seek(topicPartition, offset)

print ('Consuming messages')

for _ in xrange(10):
    for message in consumer:
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        # print ("Topic= %s : Partition= %d : Offset= %d: key= %s value= %s" % (message.topic, message.partition,
        #                                                                       message.offset, message.key,'meh'))

        reader = DataFileReader(io.BytesIO(message.value), DatumReader())
        for data in reader:
            print data
        reader.close()
        break

print('Done')