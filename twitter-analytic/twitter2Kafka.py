
import tweepy
import json
import os
from datetime import datetime
from kafka import KafkaProducer
import json 
from afinn import Afinn


producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x:json.dumps(x).encode('utf-8'))
afinn = Afinn()

print('Producer is Ready')

class MyStreamListener(tweepy.StreamListener):
    def on_error(self, status_code):
        if status_code == 420:
            print("------------ limit exceeds ------------")
            return False

    def on_status(self, status):
        tweet = dict()
        tweet['id'] = status.id_str
        tweet['time'] = status.timestamp_ms
        tweet_text = ""
        try:
            tweet_text = status.extended_tweet.full_text
        except:
            tweet_text = status.text
        tweet['text'] = tweet_text
        if len(tweet_text) > 0:
            tweet['sentiment_score'] = afinn.score(tweet_text)
            print(tweet)
            # producer.send(topic = 'twitter-raw', value=tweet)
            producer.send(topic = 'twitter-sentiment', value=tweet)


if __name__ == '__main__':
    consumer_key = "aG3qbEMLaQ86mFdHrrhMXNUpO"
    consumer_secret = "pY024y5W6hYwLPpTyCM8XyQIdO3vbUEwbLARVHoC3l5Qq7ty3z"
    access_token = "1363420936783884294-monikCcxjinRIOs49U3EQbATBkBYC6"
    access_token_secret = "KDh3Ni6lxi3fAoHJoRD1fCQ2czONQl5Gbk0KKke0rPWvN"

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth)

    myStreamListener = MyStreamListener()
    myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)

    myStream.filter(track=['the', 'i', 'to', 'a', 'and', 'is', 'in', 'it', '&'], languages=['en'])