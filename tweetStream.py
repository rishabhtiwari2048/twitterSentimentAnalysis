import tweepy
import time
import boto3
import json

class TweetStream(tweepy.StreamListener):
    def __init__(self, time_limit):
        self.start_time = time.time()
        self.limit = time_limit
        super(TweetStream, self).__init__()

    def on_data(self, raw_data):
        if (time.time() - self.start_time) < self.limit:
            print(raw_data)
            tweet_json = json.loads(raw_data)
            print(tweet_json['created_at'],
                  tweet_json['user']['screen_name'],
                  tweet_json['user']['location'])
            boto3.client('kinesis', region_name='us-west-2').put_record(
                StreamName='tweet_input_stream',
                Data=raw_data,
                PartitionKey="PartitionKey"
            )
        
            return True
        else:
            return False

    def on_error(self, status_code):
        print(status_code)
        return False


if __name__ == '__main__':
    apiKey = input("Please provide API Key \n")
    apiSecret = input("Please provide API Secret \n")
    accessToken = input("Please provide Access Token \n")
    accessSecret = input("Please provide Access Secret \n")

    auth = tweepy.OAuthHandler(apiKey, apiSecret)
    auth.set_access_token(accessToken, accessSecret)
    api = tweepy.API(auth)
    tweetStream = TweetStream(300)
    tweets = tweepy.Stream(auth=api.auth, listener=tweetStream)
    tweets.filter(track=['covid19'], stall_warnings=True)
