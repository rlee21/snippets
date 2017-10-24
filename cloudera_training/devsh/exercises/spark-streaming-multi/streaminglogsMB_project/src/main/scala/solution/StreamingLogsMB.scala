package solution

import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object StreamingLogsMB {
  
  // Given an array of new counts, add up the counts 
  // and then add them to the old counts and return the new total
  def updateCount = (newCounts: Seq[Int], state: Option[Int]) => {
     val newCount = newCounts.foldLeft(0)(_ + _)
     val previousCount = state.getOrElse(0)
     Some(newCount + previousCount) 
  }
  
  
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: solution.StreamingLogsMB <hostname> <port>")
      System.exit(1)
    } 
 
    // get hostname and port of data source from application arguments
    val hostname = args(0)
    val port = args(1).toInt

    // Create a Spark Context
    val sc = new SparkContext()

    // Set log level to ERROR to avoid distracting extra output
    sc.setLogLevel("ERROR")

    // Configure the Streaming Context with a 1 second batch duration
    val ssc = new StreamingContext(sc,Seconds(1))

    // Create a DStream of log data from the server and port specified   
    val logs = ssc.socketTextStream(hostname,port)

    // Every two seconds, display the total number of requests over the 
    // last 5 seconds
    logs.countByWindow(Seconds(5),Seconds(2)).print()

    // Bonus: Display the top 5 users every second

    // Enable checkpointing (required for all window and state operations)
    ssc.checkpoint("logcheckpt")
    
    // Count requests by user ID for every batch
    val userreqs = logs.
       map(line => (line.split(" ")(2),1)).
       reduceByKey((x,y) => x+y)

    // Update total user requests
    val totalUserreqs = userreqs.updateStateByKey(updateCount)

    // Sort each state RDD by hit count in descending order    
    val topUserreqs=totalUserreqs.
       map(pair => pair.swap).
       transform(rdd => rdd.sortByKey(false)).
       map(pair => pair.swap)

    topUserreqs.print()
    


    ssc.start()
    ssc.awaitTermination()
  }

}