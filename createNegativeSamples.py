import pandas as pd

from preprocessing import Preprocessing


def create_negative_samples(hollywood_data, tweet_data_before, me_too_tweet_per_author):
    preprocess_obj_1 = Preprocessing(hollywood_data, tweet_data_before)

    frame = preprocess_obj_1.get_author_data_for_tweets()

    print(len(frame))

    frame = frame[frame['author_id'].isin(tweet_per_author.keys())]

    print(len(frame))


if __name__ == '__main__':
    hollywood_wikidata_filename = './data/hollywood_wikidata.csv'
    tweets_before_lst = ['./data/2017Jan.json', './data/2017Feb.json', './data/2017Mar.json', './data/2017Apr.json',
                         './data/2017May.json', './data/2017Jun.json']
    me_too_tweets = './data/MeToohashtagTweets3.json'

    preprocess_obj = Preprocessing(hollywood_wikidata_filename, me_too_tweets)

    tweet_per_author = preprocess_obj.get_tweet_stats_per_author()

    for item in tweets_before_lst:
        create_negative_samples(hollywood_wikidata_filename, item, tweet_per_author)
