import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

def printRDDcount(rdd): 
    print "Number of requests: "+str(rdd.count())

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: StreamingLogsKafka.py <topic>"
        sys.exit(-1)
    
    topic = sys.argv[1]
     
    sc = SparkContext()   
    sc.setLogLevel("ERROR")

    # Configure the Streaming Context with a 1 second batch duration
    ssc = StreamingContext(sc,1)

    # Create a DStream of log data from Kafka topic 
    kafkaStream = KafkaUtils.\
       createDirectStream(ssc, [topic], {"metadata.broker.list": "localhost:9092"})

    # The weblog data is in the form (key, value), map to just the value
    logs = kafkaStream.map(lambda (key,line): line)

    # To test, print the first few lines of each batch of messages to confirm receipt
    logs.pprint()
        
    # Print out the count of each batch RDD in the stream
    logs.foreachRDD(lambda t,r: printRDDcount(r))

    # Save the logs
    logs.saveAsTextFiles("/loudacre/streamlog/kafkalogs")

    ssc.start()
    ssc.awaitTermination()
