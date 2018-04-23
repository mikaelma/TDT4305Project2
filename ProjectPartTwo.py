from pyspark import SparkContext
from operator import add
sc = SparkContext(appName="tweets")
some_string = "first second third fourth"
*first,last = some_string.split(" ")

distFile = sc.textFile("data/geotweets.tsv")
def f(x): return x
tweets = distFile.map(lambda line: line.split('\t'))
tweets = tweets.map(lambda tweet: (tweet[4], set([x.lower() for x in tweet[10].split(" ")])))
test_tweet = tweets.take(5)[1][1]
print(test_tweet)

test_tweet = set(test_tweet)
print('test tweet',test_tweet,'length ',len(test_tweet))
print('\n')
number_of_tweets = tweets.count()
tweets_per_place = tweets.map(lambda entry:(entry[0],1)).foldByKey(0,add)

#Flat map values: (place,[word1,word2,word3]) -> (place,word1) (place,word2) (place,word3)
tweets = tweets.flatMapValues(f)
#Combine place name and word in order to create a new key which can be used for folding counts: (place,word,1) -> (place word,1)
tweets = tweets.map(lambda entry:(entry[0]+' '+entry[1],1))
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
print(tweets.take(5))
tweet_len = len(test_tweet)
filter_tweets = tweets.map(lambda entry:(entry[0],1 if entry[1] in test_tweet else 0)).foldByKey(0,add)
filter_tweets = filter_tweets.map(lambda entry:(entry[0],0 if entry[1]<tweet_len else 1))
tweets = tweets.map(lambda entry:(entry[0],(entry[1],entry[2])))
tweets = tweets.leftOuterJoin(filter_tweets).filter(lambda entry:(entry[1][1]!=0))
tweets = tweets.map(lambda entry:(entry[0],entry[1][0][0],entry[1][0][1]))
print(tweets.collect())



def filter_by_word(entry,test_tweet):
    word = entry[1]
    if word not in test_tweet:
        return True
    else:
        return False
# (place,word,count)
def map_by_occurance(entry,test_tweet):
    word = entry[1]
    if word in test_tweet:
        return entry[0],1
    else:
        return entry[0],0


def number_of_tweets_per_place_set(number_of_tweets):
    the_set = {}
    for entry in number_of_tweets.collect():
        place = entry[0]
        count = entry[2]
        if place not in the_set:
            the_set[place] = count
        else:
            the_set[place] += count
    return the_set


def calculate_probability(entry,tweet):
    place = entry[0]
    word = entry[1]
    count = entry[2]
    count_total = entry[3]
    if word not in tweet:
        return (place,word,count,None)
    return place,word,count,count/float(count_total)



def naive_bayes(tweets,tweet,number_of_tweets):
    tweet_len = len(tweet)

    filter_tweets = tweets.map(lambda entry:(entry[0],1 if entry[1] in tweet else 0)).foldByKey(0,add)
    filter_tweets = filter_tweets.map(lambda entry:(entry[0],0 if entry[1]<tweet_len else 1))
    tweets = tweets.map(lambda entry:(entry[0],(entry[1],entry[2],entry[3])))
    tweets = tweets.leftOuterJoin(filter_tweets).filter(lambda entry:(entry[1][1]!=0))

    tweets = tweets.map(lambda entry:(entry[0],entry[1][0][0],entry[1][0][1],entry[1][0][2]))
    #Form of tweets:  (place,word,count,count_total)
    tweet_factor_one = tweets.map(lambda entry:(entry[0],entry[3]/float(number_of_tweets))).distinct()
    probability_per_place = tweets.map(lambda entry:(calculate_probability(entry,tweet)))
    probability_per_place = probability_per_place.filter(lambda entry:(entry[3] is not None))
 
    probability_per_place = probability_per_place.map(lambda entry:(entry[0],entry[3])).reduceByKey(lambda a,b:(a*b))


    tweets = tweet_factor_one.join(probability_per_place)

    tweets = tweets.map(lambda entry:(entry[0],entry[1][0]*entry[1][1]))
    tweets = tweets.filter(lambda entry:(entry[1]!=0))
    return tweets
#Map from (place,word,count) to (place,(word,count))
tweets = tweets.map(lambda entry:(entry[0],(entry[1],entry[2])))
tweets = tweets.leftOuterJoin(tweets_per_place)
tweets = tweets.map(lambda entry:(entry[0],entry[1][0][0],entry[1][0][1],entry[1][1]))
res  = naive_bayes(tweets,test_tweet,number_of_tweets)

print(res.takeOrdered(5,key=lambda entry:(-entry[1])))




sc.stop()