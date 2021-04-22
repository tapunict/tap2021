import sys
import pyspark
import string

n = input("Cores to use: ")
master = 'local[' + str(n) + ']'
conf = pyspark.SparkConf().setAppName('WordCounter').setMaster(master)
print("Spark Session Active! Appname WordCount with " + str(n) + (" cores used"))
sc = pyspark.SparkContext(conf=conf) 
spark = pyspark.sql.SparkSession(sc)
absFilename = str(sys.argv[1]).split("/")
relFilename = absFilename[len(absFilename) - 1]
logfile = "/opt/tap/spark/dataset/" + relFilename
print("Analyzing file " + relFilename)
logData = sc.textFile(logfile).cache()
print("RDD generated from textFile")
separator = raw_input("Insert word separator: ")
words = logData.flatMap(lambda x: x.split(separator))
print("File tokenized")
counter = words.filter(lambda x: x != "").count()
print("Word number computed. Word count: " + str(counter))
lines = logData.flatMap(lambda x: x.split("\n")).count()
print("Lines number computed. Line count: " + str(lines))
freqs = words.filter((lambda x: x != "")).map(lambda x: (x, 1)).reduceByKey(lambda a,b: a + b).sortBy(lambda x: x[1], False).take(10)
print("Frequency of words in file computed. Ten most used words:")
for x in freqs:
    print(x)
letters = list(string.ascii_lowercase)
histo = words.filter(lambda x: x != "").map(lambda x: ((x[0]).lower())).sortBy(lambda x: x).histogram(letters)
print("Histogram of word by initial plotted. Histogram: ")
print(histo)