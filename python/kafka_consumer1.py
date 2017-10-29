import sys
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

for _ in range(2):
    for message in consumer:
#        print ("Topic= %s : Partition= %d : Offset= %d: key= %s value= %s" % (message.topic, message.partition,
#                                                                              message.offset, message.key, None))
        reader = DataFileReader(io.BytesIO(message.value), DatumReader())
        results = []
        for data in reader:
        # with open("kafka_output.csv", "a") as f:
    #         for data in reader:
            for k, v in data.items():
    #                 if k not in 'event':
                if k == "event":
                    for i in v.values():
                        results.append(i)
                else:
                    results.append(v)
        print(results)
        reader.close()
        break
columns = ["created_by_user_id",
           "quote_id", 
           "created_at",
           "request_uuid",
           "customer_id",
           "event_timestamp",
           "event_name",
           "event_app_id"]
with open("kafka_output.csv", "a") as f:
    f.write(str.join(",", columns) + "\n")
    for result in results:
        output = str.join(",", [str(result.asDict()[column]) for column in columns])
        f.write(output + "\n")
