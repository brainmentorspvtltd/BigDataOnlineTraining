import tweepy
from tweepy import OAuthHandler, Stream

from tweepy.streaming import StreamListener
import socket
import json

consumer_key = 'rhx9Waf2fp8Uuh3jfM47nyhxG'
consumer_secret = 'MyZZXySDVJszmvTvOgHa2OHBqaDq3rSyBxtAR5RdOII1tBWqlR'
access_token = '1396401069488435201-0U1PuxQ96Uw3ZKpVYkU5XoeUNeZuKS'
access_secret = 'f18xIQmm8milNnMdJkS30cwlM5XqWXPkfc2nYoXRt5Prw'

class TweetListener(StreamListener):
    def __init__(self, client_socket):
        self.client_socket = client_socket
    
    def on_data(self, data):
        try:
            msg = json.loads(data)
            print(msg['text'].encode('utf-8'))
            self.client_socket.send(msg['text'].encode('utf-8'))
            return True
        except BaseException as ex:
            print(ex)
        return True
    
    def on_error(self, status):
        print(status)
        return True

def sendData(c_socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    twitter_stream = Stream(auth, TweetListener(c_socket))
    twitter_stream.filter(track=['dhoni'])

s = socket.socket()
host = '127.0.0.1'
port = 5555
s.bind((host, port))
print("Listening on port",port)
s.listen()
c,address = s.accept()
sendData(c)
