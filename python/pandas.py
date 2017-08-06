import pandas as pd

df = pd.read_csv('/Users/relee/PythonProjects/BigQuery/outputs/BigQuery.csv')
print(df)
# with open('/Users/relee/PythonProjects/BigQuery/outputs/BigQuery.csv','r') as infile:
    # print(infile.read())
    # pd.DataFrame(read_csv(infile))