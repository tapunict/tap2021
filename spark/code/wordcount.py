import pyspark
import string
import time
import argparse

parser = argparse.ArgumentParser("Extended Word Counter")
parser.add_argument("-f", dest="file",  required=True, help="Filepath to analyze")
parser.add_argument("-c", type=int, dest="cores", required=True, help="Number of cores used by worker")
parser.add_argument("-s", dest="separator", required=True, help="Separator used by tokenizer")

args = parser.parse_args()
master = 'local[' + str(args.cores) + ']'
conf = pyspark.SparkConf().setAppName('WordCounter').setMaster(master)
print("Spark Session Active! Appname WordCount with " + str(args.cores) + (" cores used"))
sc = pyspark.SparkContext(conf=conf) 
absFilename = args.file.split("/")
relFilename = absFilename[len(absFilename) - 1]
logfile = "/opt/tap/spark/dataset/" + relFilename
print("Analyzing file " + relFilename)
logData = sc.textFile(logfile).cache()
print("RDD generated from textFile")
separator = str(args.separator)
if separator == "w|":
    separator = " "
print("Separator: " + separator)
tok_start = time.perf_counter()
words = logData.flatMap(lambda x: x.split(separator)).filter(lambda x: x != "")
tok_end = time.perf_counter()
tok_time = tok_end - tok_start
print("File tokenized")
print("Time elapsed for Tokenization: " +  str(tok_time * 1000) + "ms")
wc_start = time.perf_counter()
counter = words.count()
wc_end = time.perf_counter()
wc_time = wc_end - wc_start
print("Word number computed. Word count: " + str(counter))
print("Time elapsed for Word Count: " + str(wc_time * 1000) + "ms")
lc_start =time.perf_counter()
lines = logData.flatMap(lambda x: x.split("\n")).count()
lc_end = time.perf_counter()
lc_time = lc_end - lc_start
print("Lines number computed. Line count: " + str(lines))
print("Time elapsed for Line Count: " + str(lc_time * 1000) + "ms")
fr_start = time.perf_counter()
freqs = words.map(lambda x: (x, 1)).reduceByKey(lambda a,b: a + b).sortBy(lambda x: x[1], False).take(10)
fr_end = time.perf_counter()
fr_time = fr_end - fr_start
print("Frequency of words in file computed. Ten most used words:")
for x in freqs:
    print(x)
print("Time elapsed for Frequency Count: " + str(fr_time * 1000) + "ms")
letters = list(string.ascii_lowercase)
hp_start = time.perf_counter()
histo = words.map(lambda x: ((x[0]).lower())).sortBy(lambda x: x).histogram(letters)
hp_end = time.perf_counter()
hp_time = hp_end - hp_start
print("Histogram of words by initial plotted. Histogram: ")
print(histo)
print("Time elapsed for Histogram Plotting: " + str(hp_time * 1000) + "ms")
total_time = tok_time + wc_time + lc_time + fr_time + hp_time
print("Total time elapsed from job start: " + str(total_time * 1000) + "ms")
sc.stop()