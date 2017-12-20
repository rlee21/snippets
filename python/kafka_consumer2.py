import sys
import io
# import avro.schema
# from avro.datafile import DataFileReader, DataFileWriter
# from avro.io import DatumReader, DatumWriter
from avro import schema, datafile, io
from kafka import KafkaConsumer

kafkaBroker = "kafka1wow.prod.avvo.com:9092"
# kafkaBroker = sys.argv[1]
topic = "SubscriptionSaved"
# topic = sys.argv[2]
group_id = "SubscriptionSaved_to_HDFS_02"
# group_id = sys.argv[3]
consumer = KafkaConsumer(topic, bootstrap_servers=kafkaBroker, enable_auto_commit=False,
                         group_id=group_id, auto_offset_reset="earliest")

for _ in xrange(10):
    for message in consumer:
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        print ("Topic= %s : Partition= %d : Offset= %d: key= %s value= %s" % (message.topic, message.partition,
                                                                              message.offset, message.key,"foo"))

        reader = datafile.DataFileReader(io.BytesIO(message.value), io.DatumReader())
        for data in reader:
                print data
            # infile = open("subscription_saved.avsc", "r")
            # schema_str = str(infile.read())
            # schema = schema.parse(schema_str)
            # rec_writer = io.DatumWriter(data)
            # df_writer = datafile.DataFileWriter(open("outfile.avro", 'wb'), rec_writer)
            #                            # ,
            #                            #          writers_schema=SCHEMA,codec="deflate")
            # df_writer.append(data)
            # df_writer.close()
        reader.close()
        break


# infile.close()