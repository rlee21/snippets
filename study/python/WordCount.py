from nltk.corpus import stopwords
stop_words = set(stopwords.words("english"))

sentence = '''
Apache Hadoop is an open-source software framework used for distributed storage and processing of very large data sets. It consists of computer clusters built from commodity hardware. All the modules in Hadoop are designed with a fundamental assumption that hardware failures are common occurrences and should be automatically handled by the framework.  The core of Apache Hadoop consists of a storage part, known as Hadoop Distributed File System (HDFS), and a processing part which is a MapReduce programming model. Hadoop splits files into large blocks and distributes them across nodes in a cluster. It then transfers packaged code into nodes to process the data in parallel. This approach takes advantage of data locality – nodes manipulating the data they have access to – to allow the dataset to be processed faster and more efficiently than it would be in a more conventional supercomputer architecture that relies on a parallel file system where computation and data are distributed via high-speed networking.
'''

word_count = {}
for word in sentence.lower().split():
    if word in word_count and word not in stop_words:
        word_count[word] += 1
    else:
        word_count[word] = 1

# print(word_count)
word_list = [(v, k) for k, v in word_count.items()]
word_list.sort(reverse=True)

for word in word_list:
    print(word[1], word[0])