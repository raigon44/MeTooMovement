import json
import pymongo
import config
import urllib.parse
import pandas as pd
from bson.json_util import dumps

final_result = []


def create_mongodb_client(username, password, databaseName):
    return pymongo.MongoClient("mongodb://" + username + ":" + password + "@localhost/" + databaseName)


def get_author_ids(meToo_tweets_file):
    frame = pd.read_csv(meToo_tweets_file)
    return list(set(frame['author_id'].tolist()))


def get_tweets_for_author(mongo_db_collection, author_id):
    # filter_query = {'author_id': author_id}
    filter_query = {"author_id": str(author_id),
                    "created_at": {"$regex": "2017-01|2017-02|2017-03|2017-04|2017-05|2017-06|2017-07|2017-08|2017-09",
                                   "$options": "i"}}
    query_result = mongo_db_collection.find(filter_query)

    for res in query_result:
        final_result.append(res)

    return


def get_tweets_from_database(client, meToo_tweets_file):
    list_of_authors = get_author_ids(meToo_tweets_file)

    db = client['celebDB']
    collection = db["cc_timelines"]
    count = 0
    for author in list_of_authors:
        get_tweets_for_author(collection, author)

    jsonString = dumps(final_result)

    with open('before_metoo.json', 'w') as fp:
        fp.write(jsonString)

    return


if __name__ == '__main__':
    client = create_mongodb_client(config.DB_READ_USER, config.DB_READ_USER_PWD, config.DB_NAME)

    get_tweets_from_database(client, 'MeTooTweetsWithoutRetweets.csv')












