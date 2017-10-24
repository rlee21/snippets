package stubs

import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder

object StreamingLogsKafka {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: stubs.StreamingLogsKafka <topic>")
      System.exit(1)
    }  
    val topic = args(0)

    val sc = new SparkContext()
    sc.setLogLevel("ERROR")
 
     // Configure the Streaming Context with a 1 second batch duration
    val ssc = new StreamingContext(sc,Seconds(1))
       
    // TODO
    println("Stub not yet implemented")

    ssc.start()
    ssc.awaitTermination()
        
  }
}
