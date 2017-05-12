import csv


with open("file1.csv", "r") as infile1:
    reader = csv.reader(infile1)
#    reader = infile1.read()
#    print(reader)
    next(reader) 
    file1_ids = [line[0] for line in reader if len(line) > 0]
#    print(file1_ids)


with open("file2.csv", "r") as infile2:
    reader = csv.reader(infile2)
    next(reader)
    results = [line for line in reader if len(line) > 0 and line[0] not in file1_ids]
#    results = []
#    for line in reader:
#        if len(line) > 0 and line[0] not in file1_ids:
#            results.append(line)

#print(results)
for result in results:
    print(",".join(result))
