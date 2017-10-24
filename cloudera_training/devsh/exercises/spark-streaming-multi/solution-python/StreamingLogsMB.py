import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Given an array of new counts, add up the counts 
# and then add the sum to the old counts and return the new total
def updateCount(newCounts, state): 
    if state == None: return sum(newCounts)
    else: return state + sum(newCounts)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: StreamingLogsMB.py <hostname> <port>"
        sys.exit(-1)
    
    # get hostname and port of data source from application arguments
    hostname = sys.argv[1]
    port = int(sys.argv[2])
     
    # Create a new SparkContext
    sc = SparkContext()

    # Set log level to ERROR to avoid distracting extra output
    sc.setLogLevel("ERROR")

    # Create and configure a new Streaming Context 
    # with a 1 second batch duration
    ssc = StreamingContext(sc,1)

    # Create a DStream of log data from the server and port specified    
    logs = ssc.socketTextStream(hostname,port)

    # Every two seconds, display the total number of requests over the 
    # last 5 seconds
    logs.countByWindow(5,2).pprint()
    
    # Bonus: Display the top 5 users every second

    # Enable checkpointing (required for state operations)
    ssc.checkpoint("logcheckpt")
    
    # Count requests by user ID for every batch
    userreqs = logs \
        .map(lambda line: (line.split(" ")[2],1)) \
        .reduceByKey(lambda v1,v2: v1+v2)

    # Update total user requests
    totalUserreqs = userreqs \
        .updateStateByKey(lambda newCounts, state: updateCount(newCounts, state))

    # Sort each state RDD by hit count in descending order    
    topUserreqs=totalUserreqs \
       .map(lambda (k,v):(v,k)) \
       .transform(lambda rdd: rdd.sortByKey(False)) \
       .map(lambda (k,v):(v,k)) 

    topUserreqs.pprint()
    
   
    ssc.start()
    ssc.awaitTermination()