# import sys
import io

from avro.datafile import DataFileReader
from avro.io import DatumReader
from kafka import KafkaConsumer

kafka_broker = "kafka1test.prod.avvo.com:9092"
#kafka_broker = "kafka1wow.prod.avvo.com:9092"
# kafka_broker = sys.argv[1]
topic = "QuoteCreated"
#topic = "SubscriptionSaved"
# topic = sys.argv[2]
group_id = "QuoteCreated_to_HDFS"
#group_id = "SubscriptionSaved_to_HDFS"
# group_id = sys.argv[3]
consumer = KafkaConsumer(topic, bootstrap_servers=kafka_broker, enable_auto_commit=False, group_id=group_id,
                         auto_offset_reset="earliest")

for _ in range(1):
    for message in consumer:
#        print ("Topic= %s : Partition= %d : Offset= %d: key= %s value= %s" % (message.topic, message.partition,
#                                                                              message.offset, message.key, None))
        reader = DataFileReader(io.BytesIO(message.value), DatumReader())
#        for key, value in reader.items():
        for data in reader:
        # with open("kafka_output.csv", "a") as f:
    #         for data in reader:
            for k, v in data.items():
    #                 if k not in 'event':
                if k == "event":
                    for i in v.values():
                        print(i)
                else:
                    print(v)
                #print(k, v)
                #results = [v for k, v in data.items() if k not in 'event']
                #print(results)
                #print("{0},{1}".format(data['quote_id'], data['customer_id']))

            # results = "{0},{1},{2},{3},{4},".format(data['request_uuid'],
            #                                        data['quote_id'],
            #                                        data['created_at'],
            #                                        data['created_by_user_id'],
            #                                        data['customer_id'])
            # print(results)
            # print(results)
                #print data['quote_id'], data['customer_id']
               #  for key, value in data.iteritems():
               # for i in data.values():
               #     print(i)
                    #print(key, value)
            # f.write(results + "\n")
        reader.close()
        break
# with open("kafka_output.csv", "a") as f:
#     f.writelines(results)
#results.to_csv("kafka_output.csv")
