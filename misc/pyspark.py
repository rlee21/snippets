# regular python
import json
json_entry = '{"name": "Oreo"}'
entry = json.loads(json_entry)
print(entry["name"])

json_entries = ['{"name": "Oreo0"}', '{"name": "Oreo1"}', '{"name": "Oreo2"}', '{"name": "Oreo3"}']
for json_entry in json_entries:
    entry = json.loads(json_entry)
    print(entry["name"])

# python list to rdd
rdd = sc.parallelize(json_entries)
rdd.map(lambda json_entry: json.loads(json_entry)).collect()
rdd.map(lambda json_entry: json.loads(json_entry)).map(lambda entry: entry["name"]).collect()
rdd.map(json.loads).map(lambda entry: entry["name"]).collect()


# json file to rdd
json_data = sc.textFile("file.json")
json_data.count()
json_data.first()
json_data.take(10)
# pipeline rdd (chained approaches)
book_entries = json_data.map(json.loads)
book_entries.filter(lambda entry: "title" in entry).map(lambda entry: entry["title"].take(10))
# find all keys in dataset
book_entries.flatMap(lambda entry: entry.keys()).distinct().collect()
book_entries.map(lambda entry: len(entry.keys()).take(100)
count_per_key = book_entries.flatMap(lambda entry: entry.keys().map(lambda key: (key, 1)).reduceByKey(lambda x, y: x + y )
# convert from rdd to python list
count_per_key.collect()

# large dataset
book_entries = json_data.map(json.loads)
book_entries.persist()
book_entries.filter(lambda book: "by_statement" in book) and "Charles Dickens" in book["by_statement"]) \
            .filter(lambda book: "title" in book).map(lambda book: book["title"]) \
            .distinct()
            .count()

# store in parquet format if you want to query the data
# flatMap example: splitting on spaces to convert strings to words
# collect() is like list(map(x * x))

numsRDD = sc.parallelize([1, 2, 3, 4])
squaredRDD = numsRDD.map(lambda x: x*x).collect()
filterednumsRDD = numsRDD.filter(lambda x: (x != 1)).collect()

linesRDD = sc.parallelize(["Hello world", "how are you"])
wordsRDD = linesRDD.flatMap(lambda x: x.split(" ")).collect()

