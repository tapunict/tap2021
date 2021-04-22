import sys
import pyspark
import string
import time

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
tok_start = time.perf_counter_ns()
words = logData.flatMap(lambda x: x.split(separator)).filter(lambda x: x != "")
print("File tokenized")
tok_end = time.perf_counter_ns()
tok_time = tok_end - tok_start
print("Time elapsed for Tokenization: " +  str(tok_time))
wc_start = time.perf_counter_ns()
counter = words.count()
wc_end = time.perf_counter_ns()
wc_time = wc_end - wc_start
print("Word number computed. Word count: " + str(counter))
print("Time elapsed for Word Count: " + str(wc_time))
lc_start =time.perf_counter_ns()
lines = logData.flatMap(lambda x: x.split("\n")).count()
lc_end = time.perf_counter_ns()
lc_time = lc_end - lc_start
print("Lines number computed. Line count: " + str(lines))
print("Time elapsed for Line Count: " + str(lc_time))
fr_start = time.perf_counter_ns()
freqs = words.map(lambda x: (x, 1)).reduceByKey(lambda a,b: a + b).sortBy(lambda x: x[1], False).take(10)
fr_end = time.perf_counter_ns()
fr_time = fr_end - fr_start
print("Frequency of words in file computed. Ten most used words:")
for x in freqs:
    print(x)
print("Time elapsed for Frequency Count: " + str(fr_time))
letters = list(string.ascii_lowercase)
hp_start = time.perf_counter_ns()
histo = words.map(lambda x: ((x[0]).lower())).sortBy(lambda x: x).histogram(letters)
hp_end = time.perf_counter_ns()
hp_time = hp_end - hp_start
print("Histogram of words by initial plotted. Histogram: ")
print(histo)
print("Time elapsed for Histogram Plotting: " + str(hp_time))
total_time = tok_time + wc_time + lc_time + fr_time + hp_time
print("Total time elapsed from job start: " + str(total_time))