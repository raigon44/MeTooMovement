import pandas as pd
import nltk
from difflib import SequenceMatcher


class MeasureSimilarity:

    def __init__(self, tweet_frame, quotes_frame):
        self.twitter_frame = tweet_frame
        self.quotes_frame = quotes_frame

    @staticmethod
    def compute_edit_distance(twitter_sub_frame, quote):
        # This function computes the edit distance
        # Here we are using the substitution cost as 1
        edit_distance_dict = {}
        for _, row in twitter_sub_frame.iterrows():
            edit_distance_dict[twitter_sub_frame['tweet_id']] = nltk.metrics.distance.edit_distance(quote, row['text'])
        best_match_dist = min(edit_distance_dict.values())
        best_match_tweet_id = list(edit_distance_dict.keys())[list(edit_distance_dict.values()).index(best_match_dist)]

        return best_match_tweet_id, best_match_dist

    @staticmethod
    def compute_lcs_distance(twitter_sub_frame, quote):
        # This function computes the least common substring
        # Length of the longest common substring is divided with the min of length of quote and tweet
        # This will take care of cases where quote have higher length tha tweets (tweets combined to quotes)
        lcs_distance_dict = {}
        for _, row in twitter_sub_frame.iterrows():
            match = SequenceMatcher(None, quote, row['text']).find_longest_match(0, len(quote), 0, len(row['text']))
            lcs_distance_dict[twitter_sub_frame['tweet_id']] = match.size / min(len(quote), len(row['text']))

        best_match_dist = min(lcs_distance_dict.values())
        best_match_tweet_id = list(lcs_distance_dict.keys())[list(lcs_distance_dict.values()).index(best_match_dist)]

        return best_match_tweet_id, best_match_dist

    @staticmethod
    def compute_jaccard_distance(twitter_sub_frame, quote, n_gram_size=3):
        # This function calculates the Jaccard distance between the given quote and all the tweets by the same author.
        # Here by default we are using the n-gram size as 3
        quote_tokens = set(nltk.ngrams(nltk.word_tokenize(quote), n=n_gram_size))
        jaccard_distance_dict = {}
        for _, row in twitter_sub_frame.iterrows():
            tweet_tokens = set(nltk.ngrams(nltk.word_tokenize(row['text']), n=n_gram_size))

            jaccard_distance_dict[twitter_sub_frame['tweet_id']] = nltk.jaccard_distance(quote_tokens, tweet_tokens)

        best_match_dist = min(jaccard_distance_dict.values())
        best_match_tweet_id = list(jaccard_distance_dict.keys())[list(jaccard_distance_dict.values()).index(best_match_dist)]

        return best_match_tweet_id, best_match_dist

    def calculate_similarity(self):

        lcs_distance_lst, lcs_tweet_id_lst, edit_distance_lst, edit_tweet_id_lst, jacc_distance_lst, jacc_tweet_id_lst = [], [], [], [], [], []

        speakers = self.twitter_frame['name'].tolist()
        quotes_frame_new_lst = []
        for speaker in speakers:
            twitter_sub_frame = self.twitter_frame[self.twitter_frame['name'] == speaker]
            quotes_sub_frame = self.quotes_frame[self.quotes_frame['speaker'] == speaker]

            for _, row in quotes_sub_frame.iterrows():
                lcs_tweet_id, lcs_distance = self.compute_lcs_distance(twitter_sub_frame, row['quotation'])
                jacc_tweet_id, jacc_distance = self.compute_jaccard_distance(twitter_sub_frame, row['quotation'], 3)
                edit_tweet_id, edit_distance = self.compute_edit_distance(twitter_sub_frame, row['quotation'])
                lcs_distance_lst.append(lcs_distance)
                lcs_tweet_id_lst.append(lcs_tweet_id)
                jacc_distance_lst.append(jacc_distance)
                jacc_tweet_id_lst.append(jacc_tweet_id)
                edit_distance_lst.append(edit_distance)
                edit_tweet_id_lst.append(edit_tweet_id)

            quotes_sub_frame['lcs_dist'] = lcs_distance_lst
            quotes_sub_frame['lcs_tweet_id'] = lcs_tweet_id_lst
            quotes_sub_frame['edit_dist'] = edit_distance_lst
            quotes_sub_frame['edit_tweet_id'] = edit_tweet_id_lst
            quotes_sub_frame['jaccard_dist'] = jacc_distance_lst
            quotes_sub_frame['jaccard_tweet_id'] = jacc_tweet_id_lst

            quotes_frame_new_lst.append(quotes_sub_frame)

        return pd.concat([quotes_frame_new_lst])


if __name__ == '__main__':
    twitter_csv_file = 'MeTooTweetsWithoutRetweets.csv'
    quotes_csv_file = ''

    twitter_frame = pd.read_csv(twitter_csv_file)
    quote_frame = pd.read_csv(quotes_csv_file)

    similarity_obj = MeasureSimilarity(twitter_frame, quote_frame)
    frame = similarity_obj.calculate_similarity()
    frame.to_csv('updates_quotes.csv')
