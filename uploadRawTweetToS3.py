import json
import base64
import boto3
import os


def lambda_handler(event, context):
    s3 = boto3.client("s3")
    for record in event['Records']:
        # Kinesis data is base64 encoded so decode here
        payload = base64.b64decode(record["kinesis"]["data"])
        json_object = json.loads(payload)
        json_string = json.dumps(json_object)
        file_string = "tweet_" + str(json_object["id_str"])
        with open("/tmp/" + file_string + ".json", "w") as tweet:
            tweet.write(json_string)
        s3.upload_file("/tmp/" + file_string + ".json", "rishabhtiwaricovid19tweets",
                       "raw_tweets/" + file_string + ".json")
        os.remove("/tmp/" + file_string + ".json")

