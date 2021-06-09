from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
import re

conf = SparkConf()
conf.setAppName("twitter-analytic")
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")
#Create streaming context with window size 2s
ssc = StreamingContext(sc,2)
# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")
# read data from port 9009
dataStream = ssc.socketTextStream("localhost",9099)

def aggregate_tags_count(new_values, total_sum):
    total_sum = total_sum if total_sum else (0,0,0,0)
    pos = [field[0] for field in new_values]
    neu = [field[1] for field in new_values]
    neg = [field[2] for field in new_values]
    count = [field[3] for field in new_values]

    return sum(pos)+total_sum[0], sum(neu)+total_sum[1], sum(neg)+total_sum[2], sum(count)+total_sum[3]

def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']

def process_rdd(time,rdd):
    print("-------------%s----------" %str(time))
    try:
        sql_context = get_sql_context_instance(rdd.context)
        #conver rdd -> row_rdd
        row_rdd = rdd.map(lambda w: Row(hashtag = w[0].encode('utf-8'),pos = w[1][0],neu = w[1][1],neg = w[1][2]))
        # create a DF from the Row RDD
        hashtags_df = sql_context.createDataFrame(row_rdd)
        # Register the dataframe as table
        hashtags_df.registerTempTable("hashtags")
        # get the top 10 hashtags from the table using SQL and print them
        hashtag_counts_df = sql_context.sql("select hashtag, hashtag_count, pos, neu, neg from hashtags order by hashtag_count desc limit 10")
        hashtag_counts_df.show()
        # call this method to prepare top 10 hashtags DF and send them
        # send_df_to_dashboard(hashtag_counts_df)
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)
def split_word(line):
    data = list(line.split("||||"))
    # filter out some character and normalize tweet
    tweet = re.sub('[^A-Za-z0-9# ]+', '', data[1]).lower()
    if data[0] == "POSITIVE":
        result = map(lambda x: (x, 1, 0, 0), tweet.split(" "))
    elif data[0] == "NEUTRAL":
        result = map(lambda x: (x, 0, 1, 0), tweet.split(" "))
    else:
        result = map(lambda x: (x, 0, 0, 1), tweet.split(" "))

    return list(result)

#split each tweet into words
words = dataStream.flatMap(split_word)
# filter the words to get only hashtags, then map each hashtag to its sentiment and 1 (hashtag, Positive, Neutral, Negative, 1) 
hashtag = words.filter(lambda w: w[0].startswith("#")).map(lambda x: (x[0],(x[1],x[2],x[3],1)))
#adding count for each hashtag to its last count  
tags_total = hashtag.updateStateByKey(aggregate_tags_count)
# do processing for each RDD generated in each interval
tags_total.foreachRDD(process_rdd)

print(tags_total.count())

# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()