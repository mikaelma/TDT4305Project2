from pyspark import SparkContext
from operator import add
sc = SparkContext(appName="tweets")
some_string = "first second third fourth"
*first,last = some_string.split(" ")
print('first',first)
print('last',last)
distFile = sc.textFile("data/geotweets.tsv")
def f(x): return x
tweets = distFile.map(lambda line: line.split('\t'))
tweets = tweets.map(lambda tweet: (tweet[4], tweet[10].split(" ")))
test_tweet = tweets.take(5)[1][1]
print('test tweet',test_tweet)

#Flat map values: (place,[word1,word2,word3]) -> (place,word1) (place,word2) (place,word3)
tweets = tweets.flatMapValues(f)
#Combine place name and word in order to create a new key which can be used for folding counts: (place,word,1) -> (place word,1)
tweets = tweets.map(lambda entry:(entry[0]+' '+entry[1].lower(),1))
#Fold values by key: (place word,1) (place word,1) -> (place word, 2) //With 2 beeing the count of occurences
tweets = tweets.foldByKey(0,add)
def split_back(entry):
    count = entry[1]
    #Dark python magic which puts the last word in the array in the word variable, and the rest of the items in the array are put in the place variable
    *place,word = entry[0].split(" ")
    place = " ".join(place)
    return (place,word,count)
#Make place the only part of the key again: (place word,count) -> (place,word,count) 
tweets = tweets.map(split_back)
#We only want words which occurs in the tweet, so we filter out every (place,word,count) that are not relevant
#First we make word into the key
tweets = tweets.map(lambda entry:(entry[1],entry[0],entry[2]))
print(tweets.take(10))
def filter_by_word(entry,test_tweet):
    word = entry[0]
    if word in test_tweet:
        return True
    else:
        return False
tweets = tweets.filter(lambda entry:(filter_by_word(entry,test_tweet)))
print(tweets.take(10))





sc.stop()
