package solution

import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder

object StreamingLogsKafka {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: solution.StreamingLogsKafka <topic>")
      System.exit(1)
    }  
    val topic = args(0)
    
    val sc = new SparkContext()
    sc.setLogLevel("ERROR")

    // Configure the Streaming Context with a 1 second batch duration
    val ssc = new StreamingContext(sc,Seconds(1))

    // Create a DStream of log data from Kafka topic  
    val kafkaStream = KafkaUtils.
       createDirectStream[String,String,StringDecoder,StringDecoder](ssc, 
       Map("metadata.broker.list"->"localhost:9092"), 
       Set(topic))

    // The weblog data is in the form (key, value), map to just the value
    val logs = kafkaStream.map(pair => pair._2)

    // To test, print the first few lines of each batch of messages to confirm receipt
    logs.print()
        
    // Print out the count of each batch RDD in the stream
    logs.foreachRDD(rdd => println("Number of requests: " + rdd.count()))


    // Save the logs
    logs.saveAsTextFiles("/loudacre/streamlog/kafkalogs")
    
    ssc.start()
    ssc.awaitTermination()
  }
}