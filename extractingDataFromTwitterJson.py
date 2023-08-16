import pandas as pd
import json
from bson import json_util
import re


def create_data_frame(file_name):
    with open(file_name, encoding="utf8") as fp:
        bson_data = fp.read()

        json_data = re.sub(r'ObjectId\s*\(\s*\"(\S+)\"\s*\)',
                           r'{"$oid": "\1"}',
                           bson_data)
        json_file = json.loads(json_data, object_hook=json_util.object_hook)

    author_id, tweet_id, text, retweet_cnt, reply_cnt, quote_cnt, like_cnt, referenced_tweet_type, src, created_at, hash_tags = (
    [] for i in range(11))

    for record in json_file:
        author_id.append(record['author_id'])
        created_at.append(record['created_at'])
        src.append(record['source'])
        tweet_id.append(record['id'])
        text.append(record['text'])
        retweet_cnt.append(record['public_metrics']['retweet_count'])
        reply_cnt.append(record['public_metrics']['reply_count'])
        like_cnt.append(record['public_metrics']['like_count'])
        quote_cnt.append(record['public_metrics']['quote_count'])

        if 'referenced_tweets' in record:
            temp = []
            # for item in record['referenced_tweets']:
            #     temp.append(item['type'])
            # referenced_tweet_type.append(temp)
            referenced_tweet_type.append(record['referenced_tweets'][0]['type'])
        else:
            referenced_tweet_type.append(None)

        if 'entities' in record and 'hashtags' in record['entities']:
            temp = []
            for item in record['entities']['hashtags']:
                temp.append(item['tag'])

            hash_tags.append(temp)

        else:
            hash_tags.append(None)

    frame = pd.DataFrame(
        {
            'tweet_id': tweet_id,
            'text': text,
            'author_id': author_id,
            'ref_tweet_type': referenced_tweet_type,
            'source': src,
            'created_at': created_at,
            'retweet_cnt': retweet_cnt,
            'reply_cnt': reply_cnt,
            'like_cnt': like_cnt,
            'quote_cnt': quote_cnt,
            'hashTags': hash_tags
        }
    )

    return frame


if __name__ == '__main__':
    fileName = './data/MeToohashtagTweets3.json'
    #fileName = './MarchForOurLives/relevant_tweets_celeb.json'
    #fileName = './data/before_metoo.json'
    #fileName = './Politicians/MFOLrelevantTweetsExtractedFromDB_basedOnHashtags.json'

    # with open(fileName, encoding="utf8") as fp:
    #     bsonData = fp.read()
    #
    #     jsonData = re.sub(r'ObjectId\s*\(\s*\"(\S+)\"\s*\)',
    #                       r'{"$oid": "\1"}',
    #                       bsonData)
    #     data = json.loads(jsonData, object_hook=json_util.object_hook)

    result_frame = create_data_frame(fileName)

    #result_frame.to_csv('./data/MeTooTweetsPhase1Ver1.csv')
    #result_frame.to_csv('./MarchForOurLives/MeTooTweetsPhase1Ver1.csv')
    #result_frame.to_csv('./data/BeforeMeToo.csv')
    result_frame.to_csv('./data/MFOLPolitician.csv')
