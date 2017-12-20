# import the webpage table from MySQL to HDFS, store in Parquet format

sqoop import   --connect jdbc:mysql://localhost/loudacre   --username training --password training   --table webpage   --target-dir /loudacre/webpage   --as-parquetfile
