import pandas as pd
import utilities
from preprocessing import Preprocessing
from sklearn.model_selection import train_test_split
import extractingDataFromTwitterJson


def create_negative_samples(hollywood_data, tweet_data_before, me_too_tweet_per_author, n):
    preprocess_obj_1 = Preprocessing(hollywood_data, tweet_data_before)

    frame = preprocess_obj_1.get_author_data_for_tweets()

    print(len(frame))

    frame = frame[frame['author_id'].isin(me_too_tweet_per_author.keys())]

    return frame.sample(n=1200, random_state=1)


def create_dataset(hollywood_data, tweet_data_before_lst, me_too_related_tweets_file, num_samples):
    """This function creates the first version of the dataset which contains only tweets containing the MeToo relevant hashtags
    Additionally the dataset is also cleaned up. Two csv files are saved by this function - 1. just containing the MeToo hashtags 2. containing negative samples as well"""

    me_too_related_tweets = extractingDataFromTwitterJson.create_data_frame(me_too_related_tweets_file)
    me_too_obj = Preprocessing(hollywood_data, me_too_related_tweets)
    me_too_obj.clean_tweet_text()  #Removes URL's, Emoji, page breaks
    me_too_obj.remove_ref_tweets('retweet')
    me_too_related_tweets = me_too_obj.get_author_data_for_tweets()

    utilities.save_frame(me_too_related_tweets, 'variant1', 'MeTooTweetsWithoutRetweets')

    tweets_per_author = me_too_obj.get_tweet_stats_per_author()

    neg_frame_lst = []
    for tweet_data in tweet_data_before_lst:
        neg_frame_lst.append(create_negative_samples(hollywood_data, extractingDataFromTwitterJson.create_data_frame(tweet_data),
                                                     tweets_per_author, num_samples))

    frame_negative_samples = pd.concat(neg_frame_lst)

    not_me_too_obj = Preprocessing(hollywood_data, frame_negative_samples)
    not_me_too_obj.clean_tweet_text()
    #not_me_too_obj.remove_ref_tweets('retweet')
    frame_negative_samples = not_me_too_obj.get_author_data_for_tweets()
    utilities.save_frame(frame_negative_samples, 'variant1', 'NegativeSamples')

    frame_negative_samples = frame_negative_samples.sample(n=len(me_too_related_tweets), random_state=1)

    labels = [0] * len(frame_negative_samples)
    me_too_related_tweets['labels'] = labels

    labels = [1] * len(frame_negative_samples)
    frame_negative_samples['labels'] = labels

    combined_frame = pd.concat([me_too_related_tweets, frame_negative_samples])
    frame_train, frame_test = train_test_split(combined_frame, test_size=0.2, shuffle=True, stratify=combined_frame['labels'])

    utilities.save_frame(frame_train, 'variant1', 'MeToo_df_train')
    utilities.save_frame(frame_test, 'variant1', 'MeToo_df_test')

    return


if __name__ == '__main__':
    hollywood_wikidata_filename = './data/hollywood_wikidata.csv'
    tweets_before_lst = ['./data/2017Jan.json', './data/2017Feb.json', './data/2017Mar.json', './data/2017Apr.json',
                         './data/2017May.json', './data/2017Jun.json']
    me_too_tweets = './data/MeToohashtagTweets3.json'

    sample_size = 1200

    create_dataset(hollywood_wikidata_filename, tweets_before_lst, me_too_tweets, sample_size)



