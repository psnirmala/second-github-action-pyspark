from pyspark import SparkContext
sc=SparkContext("local[*]","word-count-test");
linesRdd=sc.textFile("words.txt")
wordsRdd=linesRdd.flatMap(lambda line:line.split(" "))
wordOccurrenceRdd=wordsRdd.map(lambda word:(word,1))
wordCountRdd=wordOccurrenceRdd.reduceByKey(lambda x,y:x+y)
arr=wordCountRdd.collect()
for a in arr:
    print(a[0],a[1])
    