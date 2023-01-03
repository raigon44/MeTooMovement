import re

import numpy as np
import pandas as pd
from extractingDataFromTwitterJson import create_data_frame
import matplotlib.pyplot as plt


class Preprocessing:

    def __init__(self, wiki_data_file, tweet_data_frame):
        self.wiki_data_frame = pd.read_csv(wiki_data_file)
        self.twitter_data_frame = tweet_data_frame

    def remove_ref_tweets(self, tweet_type):
        self.twitter_data_frame = self.twitter_data_frame[self.twitter_data_frame['ref_tweet_type'] != tweet_type]

    def remove_hashtags(self, hashtag):
        self.twitter_data_frame = self.twitter_data_frame[self.twitter_data_frame['text'].str.contains(hashtag)]

    @staticmethod
    def remove_page_breaks(text_list):
        clean_text = []
        for tx in text_list:
            clean_text.append(' '.join(tx.splitlines()))
        return clean_text

    @staticmethod
    def remove_urls(text_list):
        clean_text = []
        for tx in text_list:
            clean_text.append(re.sub(r'http\S+', '', tx))
        return clean_text

    def clean_tweet_text(self):
        self.twitter_data_frame['text'] = self.remove_page_breaks(self.twitter_data_frame['text'].tolist())
        self.twitter_data_frame['text'] = self.remove_urls(self.twitter_data_frame['text'].tolist())
        self.twitter_data_frame['text'] = self.remove_emoji(self.twitter_data_frame['text'].tolist())

    def remove_tweets_from_source(self, source):
        pass

    def remove_tweets_with_short_length(self, threshold):
        self.twitter_data_frame = self.twitter_data_frame[self.twitter_data_frame['text'].str.split().str.len().gt(threshold)]

    def extract_year(self):
        self.twitter_data_frame['year'] = [timestamp[:4] for timestamp in self.twitter_data_frame['created_at'].tolist()]

    def get_author_data_for_tweets(self):
        author_names_twitter_id_dict = dict(zip(self.wiki_data_frame.twitterUserID, self.wiki_data_frame.name))
        author_gender_twitter_id_dict = dict(zip(self.wiki_data_frame.twitterUserID, self.wiki_data_frame.gender))
        author_list_twitter_data = self.twitter_data_frame['author_id'].tolist()

        author_name_twitter = []
        author_gender_twitter = []
        for author in author_list_twitter_data:
            author_name_twitter.append(author_names_twitter_id_dict[str(author)])
            author_gender_twitter.append(author_gender_twitter_id_dict[str(author)])

        self.twitter_data_frame['name'] = author_name_twitter
        self.twitter_data_frame['gender'] = author_gender_twitter

        return self.twitter_data_frame

        #In future add optimized code to get the 'occupation' of the author

    def plot_popular_author(self, num):
        self.twitter_data_frame['name'].value_counts().nlargest(num).plot(kind='bar')
        plt.show()

    def get_tweet_stats_per_author(self):
        return dict(zip(self.twitter_data_frame['author_id'].value_counts().index.tolist(), self.twitter_data_frame['author_id'].value_counts().tolist()))

    @staticmethod
    def remove_emoji(text_lst):

        emoji_pattern = re.compile("["
                                   u"\U0001F600-\U0001F64F"  # emoticons
                                   u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                                   u"\U0001F680-\U0001F6FF"  # transport & map symbols
                                   u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                                   "]+", flags=re.UNICODE)
        clean_txt = []
        for tx in text_lst:
            clean_txt.append(emoji_pattern.sub(r'', tx))

        return clean_txt


if __name__ == '__main__':

    #obj = Preprocessing('./data/hollywood_wikidata.csv', './data/MeToohashtagTweets3.json')
    obj = Preprocessing('./data/hollywood_wikidata.csv', pd.read_csv('./MarchForOurLives/MFOL_not_processed.csv'))
    #obj.remove_ref_tweets('retweet')
    frame = obj.get_author_data_for_tweets()
    obj.plot_popular_author(15)
    tweet_distribution_dict = obj.get_tweet_stats_per_author()
    frame.to_csv('./MarchForOurLives/MFOL_frame.csv')










