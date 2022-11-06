from difflib import SequenceMatcher
import pandas as pd
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import nltk


def load_data_frame(tweet_file, quotes_file):
    frame_1 = pd.read_csv(tweet_file)
    frame_2 = pd.read_csv(quotes_file)

    return frame_1, frame_2


def compute_overlap(tweet_frame, quotes_frame):

    speakers_we_need = tweet_frame['name'].value_counts().keys().tolist()
    quotes_frame_1 = quotes_frame[quotes_frame['speaker'].isin(speakers_we_need)]

    quote = []
    results = []
    tweet_id = []
    tweet_text = []

    for speaker in speakers_we_need[:1]:

        quotes_temp = quotes_frame_1[quotes_frame_1['speaker'] == speaker]
        tweets_temp = tweet_frame[tweet_frame['name'] == speaker]

        for _, row in quotes_temp.iterrows():
            res = process.extractBests(row['quotation'], tweets_temp['text'].tolist(), scorer=fuzz.partial_ratio, score_cutoff=60, limit=1)
            if res:
                quote.append(quotes_temp[quotes_temp['quoteID'] == row['quoteID']])
                results.append(res[0][1])
                tweet_id.append(tweets_temp[tweets_temp['text'] == res[0][0]]['tweet_id'].tolist()[0])
                tweet_text.append(res[0][0])

    final_result_frame = pd.concat(quote)
    final_result_frame['tweet_id'] = tweet_id
    final_result_frame['tweet_text'] = tweet_text
    final_result_frame['Levenstien_score'] = results

    return final_result_frame


def compute_lcs_score(string1, string2):

    match = SequenceMatcher(None, string1, string2).find_longest_match(0, len(string1), 0, len(string2))

    lcs_score = match.size/len(string1)*100
    lcs_partial_sim_score = match.size/len(string2)

    return lcs_score, lcs_partial_sim_score


def compute_jaccard_similarity(string1, string2, n_gram_size):

    string1_tokens = set(nltk.ngrams(nltk.word_tokenize(string1), n=n_gram_size))
    string2_tokens = set(nltk.ngrams(nltk.word_tokenize(string2), n=n_gram_size))

    return nltk.jaccard_distance(string1_tokens, string2_tokens)


if __name__ == '__main__':

    twiiter_file = './LevenstienDistanceResults/tweet_60_clean.csv'
    quotes_file = './LevenstienDistanceResults/quote_60_clean.csv'

    #a = compute_jaccard_similarity("abb", "add dbb abb")
    frame1, frame2 = load_data_frame(twiiter_file, quotes_file)

    result_frame = compute_overlap(frame1, frame2)

    quotes_res = result_frame['quotation'].tolist()
    tweets_res = result_frame['tweet_text'].tolist()

    jaccard_score = []
    lcs_score = []
    lcs_partial_score = []

    for str1, str2 in zip(quotes_res, tweets_res):
        lcss, lcsps = compute_lcs_score(str1, str2)
        lcs_score.append(lcss)
        lcs_partial_score.append(lcsps)
        jaccard_score.append(compute_jaccard_similarity(str1, str2, 3))

    result_frame['lcs_score'] = lcs_score
    result_frame['lcs_partial_score'] = lcs_partial_score
    result_frame['jaccard_similarity'] = jaccard_score

    result_frame.to_csv('./LevenstienDistanceResults/result_clean.csv')

