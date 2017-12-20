import sys
from pyspark import SparkContext

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print >> sys.stderr, "Usage: CountJPGs.py <logfile>"
        exit(-1)
        
    sc = SparkContext()
    logfile = sys.argv[1]
    sc.setLogLevel("WARN")
    count = sc.textFile(logfile).filter(lambda line: '.jpg' in line).count()
    print "Number of JPG requests: ", count
    sc.stop()