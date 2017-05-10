import csv


with open("file1.csv", "r") as infile1:
    reader = csv.reader(infile1)
    next(reader) 
    #file1_ids = [line[0] for line in reader]
    file1_ids = []
    for line in reader:
        if line: 
            file1_ids.append(line[0])


with open("file2.csv", "r") as infile2:
    reader = csv.reader(infile2)
    next(reader) 
    results = []
    for line in reader:
        if line and line[0] in file1_ids:
            results.append(line)

for result in results:
    print(",".join(result))
