# Copy example data to HDFS home directory first:
# hdfs dfs -put $DEVSH/examples/example-data/people.json
# hdfs dfs -put $DEVSH/examples/example-data/pcodes.json
# hdfs dfs -put $DEVSH/examples/example-data/zcodes.json
# hdfs dfs -put $DEVDATA/static_data/accounts_avro/ /loudacre/

import sys
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.types import *

if __name__ == "__main__":

    sc = SparkContext()
    sc.setLogLevel("WARN")
    sqlContext = HiveContext(sc)

    # Display names of currently defined tables
    tables = sqlContext.tableNames()
    for table in tables: print table

    # Example JSON data source
    peopleDF = sqlContext.read.json("people.json")
    peopleDF.show()
    for item in peopleDF.dtypes: print item
    peopleDF.count()

    # Example Hive/Impala table data source
    deviceDF = sqlContext.read.table("device")
    deviceDF.show()
    
    # Example Avro data source
    accountsDF = sqlContext.read.format("com.databricks.spark.avro").load("/loudacre/accounts_avro")
    accountsDF.show()
        
    # JDBC example -- these two examples are equivalent  
    dbaccountsDF = sqlContext.read.format("jdbc").option("url","jdbc:mysql://localhost/loudacre").option("dbtable","accounts").option("user","training").option("password","training").load()
    dbaccountsDF = sqlContext.read.option("user","training").option("password","training").jdbc("jdbc:mysql://localhost/loudacre","accounts")

    # Example select queries -- these are equivalent
    peopleDF.select("name").show() 
    peopleDF.select(peopleDF['name']).show() 

    # Other example queries
    peopleDF.select(peopleDF['name'],peopleDF.age+10).show()

    # Example joins
    pcodesDF = sqlContext.read.json("pcodes.json")

    # Basic inner join with equal column names
    peopleDF.join(pcodesDF, "pcode").show()
    
    # Left outer join
    peopleDF.join(pcodesDF, "pcode","left_outer").show()
    
    # Join of two different columns
    zcodesDF = pcodesDF.withColumnRenamed("pcode","zip")
    peopleDF.join(zcodesDF, peopleDF.pcode == zcodesDF.zip).show()

    # RDD example: convert to (pcode,name) pair RDDs and group by pcode
    peopleRDD = peopleDF.rdd
    peopleByPCode = peopleRDD.map(lambda row: (row.pcode,row.name)).groupByKey()

    schema = StructType( [ \
            StructField("age", IntegerType(), True), \
            StructField("name", StringType(), True), \
            StructField("pcode", StringType(), True)])

    myrdd=sc.parallelize([(40, "Abram", "01601"),(16,"Lucia","87501")])
    mydf = sqlContext.createDataFrame(myrdd,schema)
    mydf.show(2)
           
    sc.stop()
    