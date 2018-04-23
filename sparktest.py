from pyspark import SparkContext
from operator import add

sc = SparkContext(appName="sparky")

d1 = [('new york','someValue1',5),('new york','somevalue2',5),('chicago','somevalue2',3),('boston','somevalue1',1),('chicago','somevalue5',4)]
d2 = [('new york', 10), ('chicago',7), ('boston',1)]
d2 = sc.parallelize(d2)
d1 = sc.parallelize(d1)
d1 = d1.map(lambda entry:(entry[0],(entry[1],entry[2])))
print(d1.collect())
print(d2.collect())
d1 = d1.leftOuterJoin(d2)
print(d1.collect())
