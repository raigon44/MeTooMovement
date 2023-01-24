import json

import pandas as pd
from googleapiclient import discovery
from googleapiclient.errors import HttpError
import config
import time
import datetime
import dateutil.parser

"""The objective of this program is to extract additional features.
There are additional features at the author level and additional features at the tweet level.
We have the following additional features:
1. Involvement of an author in the social movement
Description: Here we check the total number of MeToo tweets posted by the author out of all the MeToo tweets within 
that subgroup. This is a pure author level feature.
2. Author popularity in twitter
Description: Here we check the public metrics of the last n tweets by the author and from this we compute the
the popularity of the author in twitter when he/she posted a tweet. So this is a tweet level feature.
3. Author popularity from IMDB
Description: Here we extract the weekly author popularity from IMDB STARmeter and add this rating to each tweet.
4. Relevance of the social movement (overall)
Description: Here we extract the total number of tweets with these hashtags posted every day. And then for each
tweet, we check the date on which it was posted and retrieve the relevance of the movement in that week (either
by checking the number of tweets that week or in the last seven days).
5. Relevance of the social movement (within the sub-group)
Description: Same as the 4th feature, but for the sub-groups (Hollywood, politicians etc.).
6. Toxicity of the tweet
Description: Here we measure the toxicity in the tweet text using the perspective API"""

pd.set_option('mode.chained_assignment',None)


def get_author_involvement(social_movement_tweets: pd.DataFrame) -> pd.DataFrame:
    """
    This function returns the input frame after adding a new columns which captures the involvement of the author in the
    social movement.
    :param social_movement_tweets:
    :return:
    """
    author_tweet_cnt_dict = social_movement_tweets['author_id'].value_counts().to_dict()
    social_movement_involvement = []
    for _, row in social_movement_tweets.iterrows():
        social_movement_involvement.append(author_tweet_cnt_dict[row['author_id']]/len(social_movement_tweets))
    social_movement_tweets['author_involvement'] = social_movement_involvement

    return social_movement_tweets


def add_overall_relevance_of_movement_per_tweet(social_movement_tweets_frame: pd.DataFrame, overall_tweets_json: str) ->\
        pd.DataFrame:
    """
    This function takes as input a tweet and use the no.of social movement related tweets posted in the last five days
    and the next five days to calculate this tweet level feature.
    :param: social_movement_tweets_frame, overall_tweets_json
    :return: social_movement_tweets_frame
    """

    with open(overall_tweets_json) as fp:
        overall_tweet_count = json.load(fp)

    overall_tweet_count_frame = pd.json_normalize(overall_tweet_count)

    overall_relevance = []
    social_movement_tweets_frame['created_at'] = pd.to_datetime(social_movement_tweets_frame['created_at'])

    for _, row in social_movement_tweets_frame.iterrows():

        tweet_posted_date = str(row['created_at'].date())+'T00:00:00.000Z'

        try:
            start_index = overall_tweet_count_frame.index[overall_tweet_count_frame['start'] == tweet_posted_date].tolist()[0]
        except IndexError:
            overall_relevance.append(-1)
            continue

        if start_index < 5:
            left_index = 0
        else:
            left_index = start_index - 5

        right_index = start_index + 5

        overall_relevance.append(sum(overall_tweet_count_frame[left_index:right_index]['tweet_count'].tolist()))

    social_movement_tweets_frame['overall_relevance'] = overall_relevance

    return social_movement_tweets_frame


def add_subgroup_level_relevance_of_movement_per_tweet(social_movement_tweets_frame: pd.DataFrame) -> pd.DataFrame:
    """
    This function takes as input a tweet and use the no.of social movement related tweets posted within the subgroup in
    the last five days and the next five days to calculate this tweet level feature.
    :param social_movement_tweets_frame:
    :return: social_movement_tweets_frame
    """

    # First I need to create a json object similar to one used while calculating the overall movement relevance.
    # Then the same logic as before can be used.

    # Sort the frame by created_at column
    # Create a dictionary with the first date in the frame as first element and last date as last element.

    social_movement_tweets_frame['created_at'] = pd.to_datetime(social_movement_tweets_frame['created_at'])
    social_movement_tweets_frame = social_movement_tweets_frame.sort_values(by=['created_at']).reset_index()

    first_date = social_movement_tweets_frame.loc[0]['created_at'].date()
    last_date = social_movement_tweets_frame.loc[len(social_movement_tweets_frame) - 1].date()




def get_author_popularity_in_twitter(social_movement_tweets_frame: pd.DataFrame):

    ## 1. Get all the tweets posted by an author
    ## 2. For each tweet, get the 10 last tweets posted before it
    ## 3. Take the largest public metric out of these 10 tweets.
    ## 4. Store this a feature respective to that tweet.

    pass


def create_perspective_api_client(model_version: str, method: str):
    """
    Creates and returns a perspective API client.
    :param model_version:
    :param method:
    :return: client
    """

    client = discovery.build(
        method,
        model_version,
        developerKey=config.API_KEY,
        discoveryServiceUrl="https://commentanalyzer.googleapis.com/$discovery/rest?version="+model_version,
        static_discovery=False
    )

    return client


def get_attributes_from_perspective_api(text: str) -> dict:
    """
    Get the attributes of the tweet text from the perspective API.
    :param text:
    :return: dictionary of result from the perspective client
    """

    client = create_perspective_api_client("v1alpha1", "commentanalyzer")

    analyze_request = {
        'comment': {'text': text},
        'requestedAttributes': {
            'TOXICITY': {},
            'SEVERE_TOXICITY': {},
            'IDENTITY_ATTACK': {},
            'INSULT': {},
            'PROFANITY': {},
            'THREAT': {}
        }
    }

    return client.comments().analyze(body=analyze_request).execute()


def get_features_from_perspective_api(social_movement_tweets_frame: pd.DataFrame) -> pd.DataFrame:
    """
    This function takes as input the dataframe containing the tweets related to the social movement and returns the
    updated frame with all the attributes retrieved from the perspective API.
    :param social_movement_tweets_frame:
    :return: social_movement_tweets_frame
    """

    tweets_text_lst = social_movement_tweets_frame['text'].tolist()

    severe_toxicity = []
    toxicity = []
    threat = []
    insult = []
    identity_attack = []
    profanity = []

    counter = 0

    for tweet_text in tweets_text_lst:

        print(counter)
        try:
            result = get_attributes_from_perspective_api(tweet_text)
        except HttpError:
            severe_toxicity.append(-1)
            toxicity.append(-1)
            threat.append(-1)
            insult.append(-1)
            identity_attack.append(-1)
            profanity.append(-1)
            continue

        severe_toxicity.append(result['attributeScores']['SEVERE_TOXICITY']['summaryScore']['value'])
        toxicity.append(result['attributeScores']['TOXICITY']['summaryScore']['value'])
        threat.append(result['attributeScores']['THREAT']['summaryScore']['value'])
        insult.append(result['attributeScores']['INSULT']['summaryScore']['value'])
        identity_attack.append(result['attributeScores']['IDENTITY_ATTACK']['summaryScore']['value'])
        profanity.append(result['attributeScores']['PROFANITY']['summaryScore']['value'])

        time.sleep(1)  # Perspective API have a rate limit of 1 query per second.

        counter = counter + 1

    social_movement_tweets_frame['SEVERE_TOXICITY'] = severe_toxicity
    social_movement_tweets_frame['TOXICITY'] = toxicity
    social_movement_tweets_frame['THREAT'] = threat
    social_movement_tweets_frame['INSULT'] = insult
    social_movement_tweets_frame['IDENTITY_ATTACK'] = identity_attack
    social_movement_tweets_frame['PROFANITY'] = profanity

    return social_movement_tweets_frame


if __name__ == '__main__':

    ## Getting the author involvement in the social movement.

    # frame = pd.read_csv(r'C:\Users\raigo\PycharmProjects\MeTooMovement\CleanDataset'
    #                     r'\combined_frame_tweets_reported_and_not_reported_before_preprocessing_anything.csv')
    # frame_updated = get_author_involvement(frame)
    # print(frame_updated.head())

    # Getting the overall relevance of the social movement when the tweet was posted

    all_social_movement_tweets = pd.read_csv(r'C:\Users\raigo\PycharmProjects\MeTooMovement'
                                             r'\MeTooTweetsWithoutRetweets.csv')

    ## Getting the overall relevance of the social movement.

    # overall_social_movement_tweets_per_day_json_file = r'C:\Users\raigo\PycharmProjects\MeTooMovement\TwitterAPI\overall_metoo_count_1.json'
    #
    # all_social_movement_tweets_updated = add_overall_relevance_of_movement_per_tweet(all_social_movement_tweets, overall_social_movement_tweets_per_day_json_file)
    #
    # print(all_social_movement_tweets_updated)

    ## Getting the toxicity and other attributes of the tweet text from the perspective API

    # all_social_movement_tweets_updated = get_features_from_perspective_api(all_social_movement_tweets)
    #
    # print(all_social_movement_tweets_updated.head())
    #
    # all_social_movement_tweets_updated.to_csv('MeToo_Movement_After_Toxicity.csv')