import sys
from pyspark import SparkContext
from pyspark import SparkConf

if __name__ == "__main__":
  if len(sys.argv) < 2:
    print >> sys.stderr, "Usage: WordCount.py <file>"
    exit(-1)

  sconf = SparkConf().setAppName("Word Count").set("spark.ui.port","4141")
  sc = SparkContext(conf=sconf)

  # Set logging level to WARN to avoid distracting INFO messages in demos
  sc.setLogLevel("WARN")

  counts = sc.textFile(sys.argv[1]) \
    .flatMap(lambda line: line.split()) \
    .map(lambda w: (w,1)) \
    .reduceByKey(lambda v1,v2: v1+v2)

  for pair in counts.take(5): print pair

  sc.stop()