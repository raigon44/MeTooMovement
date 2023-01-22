import pandas as pd

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


def add_overall_relevance_of_movement_when_tweet_was_posted():
    """
    This function takes as input a tweet and use the no.of social movement related tweets posted in the last seven days
    to calculate this tweet level feature.
    :return:
    """


if __name__ == '__main__':

    # Getting the author involvement in the social movement.

    # frame = pd.read_csv(r'C:\Users\raigo\PycharmProjects\MeTooMovement\CleanDataset'
    #                     r'\combined_frame_tweets_reported_and_not_reported_before_preprocessing_anything.csv')
    # frame_updated = get_author_involvement(frame)
    # print(frame_updated.head())

    # Getting the overall relevance of the social movement when the tweet was posted

    all_social_movement_tweets = pd.read_csv(r'C:\Users\raigo\PycharmProjects\MeTooMovement'
                                             r'\MeTooTweetsWithoutRetweets.csv')


