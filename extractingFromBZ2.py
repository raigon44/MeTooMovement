import json
import time
import bz2
import csv
import io
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from difflib import SequenceMatcher
import nltk
import pandas as pd


def process_func(dict_tuple):
    line = dict_tuple[1]
    quote_id = dict_tuple[0]
    speaker_name = dict_tuple[2]
    rows = []
    twitter_frame = pd.read_csv('MeTooTweetsWithoutRetweets.csv')

    for item in line['quotations']:
        if item['quoteID'] == quote_id and item['globalTopSpeaker'] == speaker_name:

            twitter_sub_frame = twitter_frame[twitter_frame['name'] == item['globalTopSpeaker']]
            quote_tokens = set(nltk.ngrams(nltk.word_tokenize(item['quotation']), n=3))
            jaccard_distance_dict = {}
            edit_distance_dict = {}
            lcs_distance_dict = {}

            if len(twitter_sub_frame) == 0:
                continue

            for _, row in twitter_sub_frame.iterrows():

                print(item['quotation'])
                print(row['text'])
                edit_distance_dict[row['tweet_id']] = nltk.edit_distance(item['quotation'], row['text'])
                tweet_tokens = set(nltk.ngrams(nltk.word_tokenize(row['text']), n=3))
                jaccard_distance_dict[row['tweet_id']] = nltk.jaccard_distance(quote_tokens, tweet_tokens)
                match = SequenceMatcher(None, item['quotation'], row['text']).find_longest_match(0, len(item['quotation']), 0, len(row['text']))
                lcs_distance_dict[row['tweet_id']] = match.size / min(len(item['quotation']), len(row['text']))

            best_lcs_dist = max(lcs_distance_dict.values())
            best_lcs_tweet_id = list(lcs_distance_dict.keys())[list(lcs_distance_dict.values()).index(best_lcs_dist)]
            best_edit_dist = min(edit_distance_dict.values())
            best_edit_tweet_id = list(edit_distance_dict.keys())[list(edit_distance_dict.values()).index(best_edit_dist)]
            best_jaccard_dist = min(jaccard_distance_dict.values())
            best_jaccard_tweet_id = list(jaccard_distance_dict.keys())[list(jaccard_distance_dict.values()).index(best_jaccard_dist)]

            row = [line['articleID'], item['quoteID'], item['quotation'], item['leftContext'], item['rightContext'],
                   item['globalTopSpeaker'], line['date'], line['url'], best_edit_dist, best_edit_tweet_id, best_lcs_dist, best_lcs_tweet_id, best_jaccard_dist, best_jaccard_tweet_id]

            rows.append(row)
    return rows


def process_list_executor(process_func, process_lst, csv_writer, max_workers, raise_error):
    row_count = 0
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        result = executor.map(process_func, process_lst, chunksize=100)

    final_rows = []
    for res in result:
        for r in res:
            final_rows.append(r)

    for csv_row_data in final_rows:
        if csv_row_data is not None:
            row_count = row_count + 1
            csv_writer.writerow(csv_row_data)
    return row_count


def process_quotebank_data(
        data_file,
        output_file,
        columns,
        match_string_pattern_list,
        stop_after_row_count=0,
        skip=0,
        process_list_max_size=500,
        max_workers=4,
        raise_error=True,
        buffer_size=1024*1024):

    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        with bz2.open(data_file, "r") as bzinput:
            csv_writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            csv_writer.writerow(columns)

            lines = []
            quote_ids = []
            line_lst = []
            spk_lst = []

            result_count = 0
            for i, line in enumerate(io.BufferedReader(bzinput, buffer_size=buffer_size)):
                line = line.decode('utf-8')
                if i == 0:
                    continue
                if 0 < stop_after_row_count < i:
                    break

                if i % 10000 == 0 and i > 10000:
                    print(result_count)

                if i < skip:
                    continue

                line = json.loads(line)

                for quote in line['quotations']:
                    if quote['globalTopSpeaker'] not in match_string_pattern_list:
                        continue
                    quote_ids.append(quote['quoteID'])
                    line_lst.append(line)
                    spk_lst.append(quote['globalTopSpeaker'])

                if len(quote_ids) > process_list_max_size:
                    print(len(quote_ids))
                    result_count += process_list_executor(process_func, zip(quote_ids, line_lst, spk_lst), csv_writer, max_workers, raise_error)
                    break
                    quote_ids = []
                    line_lst = []
                    spk_lst = []

            result_count += process_list_executor(process_func, zip(quote_ids, line_lst, spk_lst), csv_writer, max_workers, raise_error)


if __name__ == '__main__':

    start = time.perf_counter()

    speakers = []
    with open('./data/speaker.txt', 'r', encoding='latin-1') as fp:
        for spk in fp:
            speakers.append(spk)

    speakers = [spk.strip() for spk in speakers]

    print(speakers)

    process_quotebank_data(
        data_file='./data/quotebank-2020.json.bz2',
        output_file='article_centric_1.csv',
        columns=['articleID', 'quoteID', 'quotation', 'leftContext', 'rightContext', 'globalTopSpeaker', 'date', 'url', 'editDistance', 'editDistanceTweetID', 'lcsDistance', 'lcsDistanceTweetID', 'jaccardDistance', 'jaccardDistanceTweetID'],
        match_string_pattern_list=speakers
    )

    finish_time = time.perf_counter()
    print(f'Finished in {round(finish_time - start, 2)} second(s)')



