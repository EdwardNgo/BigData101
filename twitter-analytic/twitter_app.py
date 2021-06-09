import socket
import sys
import requests
import requests_oauthlib
import json
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
API_KEY = "HiZws9xgPpQB1pEVzV2hLjdED"
API_SECRET = "nueNRsz7306VG0T1MxMjjskJOeVpjfUxzmzJh1t4ZPNBCofjj5"
ACCESS_TOKEN = "1363420936783884294-QE6Jrpsroj6GnIXHQ8XVR2lstXVnW0"
ACCESS_TOKEN_SECRET = "ZVhVV1IRfD22DvnAVPKGnpHsgjzycf2ofE6gAZvSIslaP"
my_auth = requests_oauthlib.OAuth1(API_KEY,API_SECRET,ACCESS_TOKEN,ACCESS_TOKEN_SECRET)

def get_tweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query_data = [('language', 'en'), ('follow', '1363420936783884294')]
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = requests.get(query_url, auth=my_auth, stream=True)
    print(query_url, response)
    print(response.text)
    return response

def process_tweets_to_spark(http_resp,tcp_conn):
    for line in http_resp.iter_lines():
        try:
            full_tweet = json.loads(line)
            tweet_text = str(full_tweet['text'].encode('utf-8'))

            # analysis sentiment score
            sentiment_score = analyzer.polarity_scores(tweet_text)["compound"]
            if sentiment_score >= 0.05:
                sentiment = "POSITIVE"
            elif sentiment_score <= -0.05:
                sentiment = "NEGATIVE"
            else:
                sentiment = "NEUTRAL"

            # separate sentiment label with tweet content
            mess =  sentiment + '||||' + tweet_text + '\n' 
            tcp_conn.send(bytes(mess,'utf8'))
        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)
if __name__ == '__main__':
    TCP_IP = "localhost"
    TCP_PORT = 9099
    conn = None
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((TCP_IP,TCP_PORT))
    s.listen(1)
    print("Waiting for tcp connection...")
    conn,addr = s.accept()
    print("Connected...start getting tweets")
    resp = get_tweets()
    process_tweets_to_spark(resp,conn)
