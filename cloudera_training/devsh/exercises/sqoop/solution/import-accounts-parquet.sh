sqoop import \
  --connect jdbc:mysql://localhost/loudacre \
  --username training --password training \
  --table accounts \
  --target-dir /loudacre/accounts_parquet \
  --as-parquetfile

# Substitute an actual generated file name for 'myfile' below
parquet-tools head  hdfs://localhost/loudacre/myfile.parquet
