import pandas as pd
import numpy as np


def load_data(me_too_tweets_file1, me_too_tweets_file2, all_quotes_file):

    frame_1 = pd.read_csv(me_too_tweets_file1)
    frame_2 = pd.read_csv(me_too_tweets_file2)

    me_too_tweets_frame = pd.concat([frame_1, frame_2])

    all_quotes = pd.read_csv(all_quotes_file)

    return me_too_tweets_frame, all_quotes


def compute_similarity_tweet_against_quote(me_too_tweets_frame, all_quotes):
    pass


def remove_authors(me_too_tweets_frame, all_quotes):

    me_too_authors = me_too_tweets_frame[]