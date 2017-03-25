from pyparsing import Regex, re
from pyspark import SparkContext,SparkConf
conf = SparkConf().setAppName("pagerank")
sc = SparkContext(conf=conf)

count=0.0

def outlink(li):
        if not li:
                return
        else:
                title = re.search('\<title\>(.*?)\<\/title\>',li).group(1)
                data = re.findall('\[\[(.*?)\]\]',li)
                if not data:
                        data = []
                return (title,(1.0/count,data))


def distrank(entry):
        page = entry[0]
        rank = (entry[1])[0]
        outlinks = (entry[1])[1]
        listToRet = []
        listToRet.append((page,(0.0,outlinks)))
        if not outlinks:
                return listToRet
        rankToDist = rank/len(outlinks)
        for outlink in outlinks:
                listToRet.append((outlink,(rankToDist,"")))
        return listToRet

def reddata(x,y):
        rank = x[0]+y[0]
        outlinks = []
        if x[1]:
                outlinks = x[1]
        if y[1]:
                outlinks = y[1]
        return (rank,outlinks)

lines = sc.textFile(sys.argv[1])
titles = lines.flatMap(lambda x: re.findall('\<title\>(.*?)\<\/title\>',x))
datas = lines.flatMap(lambda x: re.findall('\[\[(.*?)\]\]',x)).distinct()
links = titles.union(datas).distinct()
count = links.count()
dataMap = lines.map(lambda x: outlink(x))

for i in range(0, 2):
        mapped = dataMap.flatMap(lambda x : distrank(x))
        dataMap = mapped.reduceByKey(lambda x,y : reddata(x,y))
        dataMap = dataMap.map(lambda x: (x[0],(x[1][0]*.85+.15 ,x[1][1])))
		
dataMap.takeOrdered(100,key = lambda x: x[1][0])
orderedRank=dataMap.takeOrdered(100,key=lambda atuple: -atuple[1][0])
sc.parallelize(orderedRank).map(lambda x: x[0]+str("   ")+str(x[1][0])).saveAsTextFile("outputDir2")