import json

import tweepy
import config
from argparse import ArgumentParser
import datetime


parser = ArgumentParser()
parser.add_argument('--output', required=True)

args = parser.parse_args()


def get_client(is_wait):
    return tweepy.Client(bearer_token=config.BEARER_TOKEN, wait_on_rate_limit=is_wait)


def get_tweet_count(twitter_query, twitter_client):

    start_date = datetime.date(2017, 10, 1)  #TODO: Add these as input arguments. MFOL will have different start and end time
    end_date = datetime.date(2020, 10, 31)
    delta = datetime.timedelta(31)

    result = []
    while start_date <= end_date:

        end_time = start_date+delta

        response_collection = tweepy.Paginator(twitter_client.get_all_tweets_count, query=twitter_query,
                                               granularity='day', start_time=start_date.strftime("%Y-%m-%d")+'T00:00:00Z', end_time=end_time.strftime("%Y-%m-%d")+'T00:00:00Z')

        for response in response_collection:

            for item in response.data:
                result.append(item)

        start_date += delta

    json_string = json.dumps(result)
    with open(args.output, 'w') as out_fp:
        out_fp.write(json_string)
    return


if __name__ == '__main__':

    client = get_client(True)

    query_variant_1 = '#metoo OR #timesup OR #everydaysexism OR #sexualharasment OR #wheniwas OR #notokay OR #whyididntreport OR ' \
            '#nomoore OR #nevermore OR #meat14 OR #believesurvivors OR #sexualassualt OR #MeToo OR #METOO OR #TimesUp OR ' \
            '#WhyIDidntReport OR #NoMoore OR #NeverMoore OR #MeAt14 OR #whenIwas'   #TODO: Add the query to a file and pass it as an argument

    get_tweet_count(query_variant_1, client)



