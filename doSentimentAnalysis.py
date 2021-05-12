import json
import urllib.parse
import boto3
import os
import re

print('Loading function')
comprehend = boto3.client(service_name='comprehend', region_name='us-west-2', use_ssl=True)
s3 = boto3.client(service_name='s3', region_name='us-west-2', use_ssl=True)
sns = boto3.client(service_name='sns', region_name='us-west-2', use_ssl=True)


def calculateSentiment(tweet, language):
    raise_alarm = 0
    tweet_string = re.sub(r"[^a-zA-Z0-9]+", ' ', tweet)
    comprehend_response = comprehend.detect_sentiment(Text=tweet_string, LanguageCode="en")
    comprehend_entities = comprehend.detect_entities(Text=tweet_string, LanguageCode="en")
    sentiment = comprehend_response['Sentiment']
    sentiment_score = comprehend_response['SentimentScore'][sentiment.capitalize()]
    keywords = list()
    print("Sentiment {} and sentiment score {}".format(sentiment, sentiment_score))
    for i in comprehend_entities['Entities']:
        keywords.append(i['Text'])
    print(keywords)
    if sentiment.upper() == 'NEGATIVE' and sentiment_score * 100.0 > 85.0:
        raise_alarm = 1

    return (sentiment, sentiment_score, keywords, raise_alarm)


def send_notification(user_name, tweet, tweet_id, sentiment_score):
    sns.publish(TopicArn='arn:aws:sns:us-west-2:152669713710:negative_review_alert',
                Message=tweet,
                Subject=user_name + "@tweet_" + tweet_id)


def lambda_handler(event, context):
    for record in event['Records']:
        try:
            bucket = record['s3']['bucket']['name']
            key = urllib.parse.unquote_plus(record['s3']['object']['key'])
            tmpKey = key.split('/')[1]
            downloaded_tweet = '/tmp/download_{}'.format(tmpKey)
            uploaded_tweet = '/tmp/upload_{}'.format(tmpKey)
            s3.download_file(bucket, key, downloaded_tweet)
            with open(downloaded_tweet, "r") as downloaded_file:
                tweet = json.load(downloaded_file)
            print(tweet)
            tweet_sentiment_data = calculateSentiment(tweet["text"], tweet["final_language"])

            if tweet_sentiment_data[-1] == 1:
                send_notification(tweet["screen_name"], tweet["text"], tweet["id_str"], tweet_sentiment_data[1])

            tweet_dict = {"id_str": tweet["id_str"],
                          "weekday": tweet["weekday"],
                          "created_date": tweet["created_date"],
                          "created_time": tweet["created_time"],
                          "user_location": tweet["user_location"],
                          "screen_name": tweet["screen_name"],
                          "verified": tweet["verified"],
                          "followers_count": tweet["followers_count"],
                          "friends_count": tweet["friends_count"],
                          "text": tweet["text"],
                          "sentiment": tweet_sentiment_data[0],
                          "sentiment_score": tweet_sentiment_data[1],
                          "keywords": tweet_sentiment_data[2],
                          "original_language": tweet["original_language"],
                          "final_language": tweet["final_language"]
                          }
            json_string = json.dumps(tweet_dict, indent=4)
            with open(uploaded_tweet, "w") as file:
                file.write(json_string)
            s3.upload_file(uploaded_tweet, "rishabhtiwaricovid19tweets", "ready_for_athena/" + tmpKey)
            os.remove(downloaded_tweet)
            os.remove(uploaded_tweet)

        except Exception as e:
            print(e)
            print(
                'Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(
                    key, bucket))
            raise e
