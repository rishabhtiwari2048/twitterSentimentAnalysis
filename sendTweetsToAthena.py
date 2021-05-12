import json
import urllib.parse
import boto3
import os

print('Loading function')

s3 = boto3.client('s3')
translate = boto3.client(service_name='translate', region_name='us-west-2', use_ssl=True)


class Translate_lang:
    def __init__(self):
        self.t_lang = {'Afrikaans': 'af', 'Albanian': 'sq', 'Amharic': 'am',
                       'Arabic': 'ar', 'Armenian': 'hy', 'Azerbaijani': 'az',
                       'Bengali': 'bn', 'Bosnian': 'bs', 'Bulgarian': 'bg',
                       'Catalan': 'ca', 'Chinese (Simplified)': 'zh',
                       'Chinese (Traditional)': 'zh-TW', 'Croatian': 'hr',
                       'Czech': 'cs', 'Danish': 'da ', 'Dari': 'fa-AF',
                       'Dutch': 'nl ', 'English': 'en', 'Estonian': 'et',
                       'Farsi (Persian)': 'fa', 'Filipino Tagalog': 'tl',
                       'Finnish': 'fi', 'French': 'fr', 'French (Canada)': 'fr-CA',
                       'Georgian': 'ka', 'German': 'de', 'Greek': 'el', 'Gujarati': 'gu',
                       'Haitian Creole': 'ht', 'Hausa': 'ha', 'Hebrew': 'he ', 'Hindi': 'hi',
                       'Hungarian': 'hu', 'Icelandic': 'is', 'Indonesian': 'id ', 'Italian': 'it',
                       'Japanese': 'ja', 'Kannada': 'kn', 'Kazakh': 'kk', 'Korean': 'ko',
                       'Latvian': 'lv', 'Lithuanian': 'lt', 'Macedonian': 'mk', 'Malay': 'ms',
                       'Malayalam': 'ml', 'Maltese': 'mt', 'Mongolian': 'mn', 'Norwegian': 'no',
                       'Persian': 'fa', 'Pashto': 'ps', 'Polish': 'pl', 'Portuguese': 'pt',
                       'Romanian': 'ro', 'Russian': 'ru', 'Serbian': 'sr', 'Sinhala': 'si',
                       'Slovak': 'sk', 'Slovenian': 'sl', 'Somali': 'so', 'Spanish': 'es',
                       'Spanish (Mexico)': 'es-MX', 'Swahili': 'sw', 'Swedish': 'sv',
                       'Tagalog': 'tl', 'Tamil': 'ta', 'Telugu': 'te', 'Thai': 'th',
                       'Turkish': 'tr', 'Ukrainian': 'uk', 'Urdu': 'ur', 'Uzbek': 'uz',
                       'Vietnamese': 'vi', 'Welsh': 'cy'}

    def check_lang_code_in_translate(self, given_lang):
        if given_lang in list(self.t_lang.values()):
            return True
        else:
            return False


def translate_tweet(tweet, language_code):
    t1 = Translate_lang()
    if language_code != "en":
        if t1.check_lang_code_in_translate(language_code):
            language_supported = 1
            result = translate.translate_text(Text=tweet, SourceLanguageCode=language_code, TargetLanguageCode="en")
            return (result.get('TranslatedText'), language_supported)
        else:
            language_supported = 0
            return (tweet, language_supported)
    else:
        language_supported = 1
        return (tweet, language_supported)


def lambda_handler(event, context):
    # print("Received event: " + json.dumps(event, indent=2))

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
            date_and_time = tweet["created_at"]
            date = date_and_time.split()[0]
            time = date_and_time.split()[1].split('+')[0]
            print(date)
            print(time)
            translated_tweet = translate_tweet(tweet["text"], tweet["language"])
            print(translated_tweet)
            if translated_tweet[1] == 1:

                tweet_dict = {"id_str": tweet["id_str"],
                              "weekday": tweet["weekday"],
                              "created_date": date,
                              "created_time": time,
                              "user_location": tweet["user_location"],
                              "screen_name": tweet["screen_name"],
                              "verified": tweet["verified"],
                              "followers_count": tweet["followers_count"],
                              "friends_count": tweet["friends_count"],
                              "text": translated_tweet[0],
                              "original_language": tweet["language"],
                              "final_language": "en"
                              }
                json_string = json.dumps(tweet_dict, indent=4)
                with open(uploaded_tweet, "w") as file:
                    file.write(json_string)
                s3.upload_file(uploaded_tweet, "rishabhtiwaricovid19tweets", "ready_for_analysis/" + tmpKey)
                os.remove(downloaded_tweet)
                os.remove(uploaded_tweet)
            else:
                json_string = json.dumps(tweet, indent=4)
                with open(uploaded_tweet, "w") as file:
                    file.write(json_string)
                s3.upload_file(uploaded_tweet, "rishabhtiwaricovid19tweets", "bad_tweets/" + tmpKey)
                os.remove(downloaded_tweet)
                os.remove(uploaded_tweet)

        except Exception as e:
            print(e)
            print(
                'Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(
                    key, bucket))
            raise e
