import json
import pandas as pd


def extract_user_data(json_file):
    """This functions takes the json file containing user data as the input and returns a data frame containing all
    the relevant user information. """

    with open(json_file, 'r', encoding="utf-8") as fp:
        json_data = json.load(fp)

    author_id = []
    author_name = []
    follower_cnt = []
    following_cnt = []
    tweet_cnt = []
    verified_account = []

    for author_data in json_data:
        follower_cnt.append(author_data['public_metrics']['followers_count'])
        following_cnt.append(author_data['public_metrics']['following_count'])
        tweet_cnt.append(author_data['public_metrics']['tweet_count'])
        verified_account.append(author_data['verified'])
        author_id.append(author_data['id'])
        author_name.append(author_data['name'])

    twitter_author_frame = pd.DataFrame(columns=['author_id', 'author_name', 'follower_cnt', 'following_cnt',
                                                 'tweet_cnt', 'verified'])

    twitter_author_frame['author_id'] = author_id
    twitter_author_frame['author_name'] = author_name
    twitter_author_frame['follower_cnt'] = follower_cnt
    twitter_author_frame['following_cnt'] = following_cnt
    twitter_author_frame['tweet_cnt'] = tweet_cnt
    twitter_author_frame['verified'] = verified_account

    twitter_author_frame.to_csv('twitter_authors_politicians.csv')

    return


if __name__ == '__main__':

    extract_user_data('cc_users.json')
    extract_user_data('pp_users.json')










