package solution

import org.apache.spark.SparkContext

object CountJPGs {
   def main(args: Array[String]) {
     if (args.length < 1) {
       System.err.println("Usage: solution.CountJPGs <logfile>")
       System.exit(1)
     }

     val sc = new SparkContext()

     val logfile = args(0)
     
     sc.setLogLevel("WARN")
     
     val jpgcount = sc.textFile(logfile).
        filter(line => line.contains(".jpg")).
        count()

     println( "Number of JPG requests: " + jpgcount)

     sc.stop
   }
 }

