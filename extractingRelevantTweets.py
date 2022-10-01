import re

import numpy as np
import pandas as pd
import json
from bson import json_util


def extract_tweets(keyword_list, tweets_list):
    rel = []
    for item in tweets_list:
        # if any(word in item for word in keyword_list):
        #     rel.append(item)
        temp = item
        item = item.split(' ')
        item = [s.lower() for s in item]

        for k in keyword_list:

            if k in item:
                rel.append(' '.join(item))

    return rel


if __name__ == '__main__':

    # with open('myfile3.json', encoding="utf8") as f:
    #     data = json.loads(f.read(), object_hook=json_util.object_hook())
    #     #data = json.load(data)
    # text = []
    # for record in data:
    #     #record = json.loads(json_util.dumps(record))
    #     print(type(record))
    #     print(record)
    #     exit(0)
    #     text.append(record['text'])

    with open('myfile3.json', "r", encoding="utf8") as f:
        bsondata = f.read()

        jsondata = re.sub(r'ObjectId\s*\(\s*\"(\S+)\"\s*\)',
                          r'{"$oid": "\1"}',
                          bsondata)
        data = json.loads(jsondata, object_hook=json_util.object_hook)

    text = []
    for record in data:

        text.append(record['text'])

    keyWords = np.load('relevantKeywords.npy').tolist()

    relevant_text = extract_tweets(keyWords, text)

    print(len(relevant_text))

    df = pd.DataFrame()
    df['text'] = relevant_text

    df.to_csv('temp4.csv')

    df_milano = pd.DataFrame()
    df_milano['text'] = text

    df_milano.to_csv('Milano.csv')

