import json

import tweepy
import config
from argparse import ArgumentParser


parser = ArgumentParser()
parser.add_argument('--output', required=True)

args = parser.parse_args()


def get_client(is_wait):
    return tweepy.Client(bearer_token=config.BEARER_TOKEN, wait_on_rate_limit=is_wait)


def get_tweet_count(twitter_query, twitter_client):
    for response in tweepy.Paginator(client.get_all_tweets_count,
                                     query=twitter_query,
                                     granularity='day',
                                     start_time='2017-10-01T00:00:00Z',
                                     end_time='2020-11-30T00:00:00Z'):

        json_string = json.dumps(response.data)
        with open(args.output, 'w') as out_fp:
            out_fp.write(json_string)

        return


if __name__ == '__main__':

    client = get_client(True)

    query_variant_1 = '#metoo OR #timesup OR #everydaysexism OR #sexualharasment OR #wheniwas OR #notokay OR #whyididntreport OR ' \
            '#nomoore OR #nevermore OR #meat14 OR #believesurvivors OR #sexualassualt OR #MeToo OR #METOO OR #TimesUp OR ' \
            '#WhyIDidntReport OR #NoMoore OR #NeverMoore OR #MeAt14 OR #whenIwas'

    get_tweet_count(query_variant_1, client)



