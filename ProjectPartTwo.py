from pyspark import SparkContext
sc = SparkContext(appName="tweets")

distFile = sc.textFile("data/geotweets.tsv")

tweets = distFile.map(lambda line: line.split('\t'))
tweetolini = tweets.map(lambda tweet: (tweet[4], tweet[10]))
print(tweetolini.take(10))

sc.stop()
