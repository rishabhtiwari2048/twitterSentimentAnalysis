import json
import urllib.parse
import boto3
import os
import datetime
from dateutil import tz

print('Loading function')

s3 = boto3.client('s3')


def dateTimeFormatter(created_at):
    weekdays = {'Sun': 'Sunday', 'Mon': 'Monday', 'Tue': 'Tuesday', 'Wed': 'Wednesday', 'Thu': 'Thursday',
                'Fri': 'Friday', 'Sat': 'Saturday'}
    months = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
              'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}
    dateTimeList = created_at.split()
    timeList = dateTimeList[3].split(':')
    UTC = tz.gettz('Africa/Abidjan')
    IST = tz.gettz('Asia/Kolkata')
    default_time = datetime.datetime(int(dateTimeList[-1]), int(months[dateTimeList[1]]), int(dateTimeList[2]),
                                     int(timeList[0]), int(timeList[1]), int(timeList[2]), tzinfo=UTC)
    indian_time = default_time.astimezone(IST)
    return (weekdays[dateTimeList[0]], str(indian_time))


def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))

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
                date_and_time = dateTimeFormatter(tweet["created_at"])
                print(date_and_time)
                tweet_dict = {"id_str": tweet["id_str"],
                              "weekday": date_and_time[0],
                              "created_at": date_and_time[1],
                              "user_location": tweet["user"]["location"],
                              "screen_name": tweet["user"]["screen_name"],
                              "verified": tweet["user"]["verified"],
                              "followers_count": tweet["user"]["followers_count"],
                              "friends_count": tweet["user"]["friends_count"],
                              "text": tweet["text"],
                              "language": tweet["lang"]
                              }
                json_string = json.dumps(tweet_dict, indent=4)
            with open(uploaded_tweet, "w") as file:
                file.write(json_string)
            s3.upload_file(uploaded_tweet, "rishabhtiwaricovid19tweets", "processed_tweets/" + tmpKey)
            os.remove(downloaded_tweet)
            os.remove(uploaded_tweet)
        except Exception as e:
            print(e)
            print(
                'Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(
                    key, bucket))
            raise e
