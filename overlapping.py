import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz
from fuzzywuzzy import process


def load_data_frame(tweet_file, quotes_file):
    frame_1 = pd.read_csv(tweet_file)
    frame_2 = pd.read_csv(quotes_file)

    return frame_1, frame_2


def compute_overlap(tweet_frame, quotes_frame):

    speakers_we_need = tweet_frame['name'].value_counts().keys().tolist()
    quotes_frame_1 = quotes_frame[quotes_frame['speaker'].isin(speakers_we_need)]

    quote = []
    results = []
    tweet = []

    for speaker in speakers_we_need[2:4]:

        print(speaker)

        quotes_temp = quotes_frame_1[quotes_frame_1['speaker'] == speaker]
        tweets_temp = tweet_frame[tweet_frame['name'] == speaker]

        for _, row in quotes_temp.iterrows():
            res = process.extractBests(row['quotation'], tweets_temp['text'].tolist(), scorer=fuzz.partial_ratio, score_cutoff=90, limit=1)
            if res:
                quote.append(row)
                results.append(res)
                tweet.append(tweet_frame[tweet_frame['text'] == res[0][0]])

        print(len(res))

    tweet_res = pd.concat(tweet)

    return tweet_res


if __name__ == '__main__':

    file1 = 'MeTooTweetsWithoutRetweets.csv'
    file2 = 'dataFrame200.csv'

    frame1, frame2 = load_data_frame(file1, file2)

    result_frame = compute_overlap(frame1, frame2)

    result_frame.to_csv('result.csv')

