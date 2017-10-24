import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: StreamingLogsKafka.py <topic>"
        sys.exit(-1)
    
    topic = sys.argv[1]
     
    sc = SparkContext()   
    sc.setLogLevel("ERROR")

    # Configure the Streaming Context with a 1 second batch duration
    ssc = StreamingContext(sc,1)
        
    # TODO
    print "Stub not yet implemented"

    ssc.start()
    ssc.awaitTermination()