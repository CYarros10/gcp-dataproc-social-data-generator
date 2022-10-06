import zstandard as zstd
import time
import textstat
import re
from textblob import TextBlob
from textblob import Blobber
from better_profanity import profanity
from time import sleep
from datetime import datetime, timezone
import json
import os
import sys
from google.cloud import storage

source_compressed_file = sys.argv[1]
destination_bucket = sys.argv[2]

bot_list = ['AutoModerator', 'keepthetips', 'MAGIC_EYE_BOT',
            'Funny_Sentinel', 'Funny-Mod', 'Showerthoughts_Mod', 'autotldr',
            'art_moderator_bot', 'ApiContraption', 'WSBVoteBot', 'FittitBot',
            'Photoshopbattlesbot', 'dataisbeautiful-bot', 'timestamp_bot',
            'remindditbot', 'converter-bot', 'lntipbot']

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )

def remove_emoji(comment):
    emoji_pattern = re.compile("["
       u"\U0001F600-\U0001F64F"  # emoticons
       u"\U0001F300-\U0001F5FF"  # symbols & pictographs
       u"\U0001F680-\U0001F6FF"  # transport & map symbols
       u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
       u"\U00002702-\U00002f7B0"
       u"\U000024C2-\U0001F251"
       "]+", flags=re.UNICODE)

    cleaned_comment =  emoji_pattern.sub(r'', comment)

    return cleaned_comment

def get_comment_sentiment(comment):
    pattern_analysis = TextBlob(comment)
    return pattern_analysis.sentiment

def utc_to_local(utc_dt):
    return utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=None)


def modify(object):
    commentbody = object['body']
    author = object['author']
    if author not in bot_list:
        if len(commentbody) > 0 and len(commentbody) < 500:
            if profanity.contains_profanity(str(commentbody)):
                is_censored = 1
            else:
                is_censored = 0
            cleaned_comment = remove_emoji(str(commentbody))
            comment_date = str(datetime.utcfromtimestamp(object['created_utc']).strftime('%Y-%m-%d %H:%M:%S'))

            #compartmentalize and localize date for easier searching
            dt = utc_to_local(datetime.strptime(comment_date, '%Y-%m-%d %H:%M:%S'))
            comment_timestamp = dt.strftime('%Y/%m/%d %H:%M:%S')

            # comment sentiment and subjectivity
            sentiment = get_comment_sentiment(cleaned_comment)
            pattern_polarity = round(sentiment.polarity,4)
            pattern_subjectivity = round(sentiment.subjectivity, 4)

            is_positive = 0
            is_neutral = 0
            is_negative = 0

            if (pattern_polarity > 0.3):
                is_positive = 1
            elif (pattern_polarity >= -0.3 and pattern_polarity <= 0.3):
                is_neutral = 1
            else:
                is_negative = 1

            is_subjective = 0
            if (pattern_subjectivity > 0.7):
                is_subjective = 1

            # Readability statistics
            comment_reading_ease_score = textstat.flesch_reading_ease(cleaned_comment)
            comment_reading_ease = ''
            if (comment_reading_ease_score >= 80):
                comment_reading_ease = 'easy'
            elif (comment_reading_ease_score > 50 and comment_reading_ease_score < 80):
                comment_reading_ease = 'standard'
            else:
                comment_reading_ease = 'difficult'

            comment_reading_grade_level = textstat.text_standard(cleaned_comment, float_output=False)

            # censor and lower
            censored_comment = profanity.censor(cleaned_comment).lower()

            commentjson = {
                            'comment_id': str(object['id']),
                            'subreddit': str(object['subreddit']),
                            'author': str(object['author']),
                            'comment_text': censored_comment,
                            'total_words': len(cleaned_comment.split()),
                            'reading_ease_score': comment_reading_ease_score,
                            'reading_ease': comment_reading_ease,
                            'reading_grade_level': comment_reading_grade_level,
                            'sentiment_score': pattern_polarity,
                            'censored': is_censored,
                            'positive': is_positive,
                            'neutral': is_neutral,
                            'negative': is_negative,
                            'subjectivity_score': pattern_subjectivity,
                            'subjective': is_subjective,
                            'url': "https://reddit.com" + object['permalink'],
                            'comment_date': comment_date,
                            'comment_timestamp': comment_timestamp,
                            'comment_hour': dt.hour,
                            'comment_year': dt.year,
                            'comment_month': dt.month,
                            'comment_day': dt.day
                        }
            return commentjson


with open(source_compressed_file, 'rb') as fh:
    dctx = zstd.ZstdDecompressor(max_window_size=2147483648)
    with dctx.stream_reader(fh) as reader:
        previous_line = ""
        file_count = 0
        comment_modified_count = 0
        while True:
            fname = source_compressed_file+"_"+str(file_count)+".json"
            f = open("/files"+fname, "a")
            chunk_count = 0
            while chunk_count < 1: # chunk_count * chunk = file_size
                chunk = reader.read(2**14)  # 16kb chunks
                if not chunk:
                    break
                try:
                    string_data = chunk.decode('utf-8')
                    lines = string_data.split("\n")
                    for i, line in enumerate(lines[:-1]):
                        if i == 0:
                            line = previous_line + line
                        object = json.loads(line)
                        # modify object here to fit other schema
                        formatted_object = modify(object)
                        comment_modified_count +=1
                        if (comment_modified_count % 1000 == 0):
                            print("comments modified = "+str(comment_modified_count))
                        final_data = json.dumps(formatted_object)
                        if final_data != 'null':
                            f.write(json.dumps(formatted_object)+"\n")
                    previous_line = lines[-1]
                    chunk_count += 1
                except:
                    print("couldn't read data. moving on...")
                    break
            f.close()
            upload_blob(destination_bucket, "/files"+fname, "comments"+fname)
            os.remove("/files"+fname)
            file_count += 1
