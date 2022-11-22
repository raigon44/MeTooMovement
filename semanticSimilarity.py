import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from sentence_transformers import SentenceTransformer, util


def compute_semantic_similarity_per_quote(quote_item):
    """computes the semantic similarity between the given quote (argument) and all the tweets posted by the same author.
    Returns the tweetID and the cosine distance of the most similar tweet."""
    assigned_speaker = quote_item['globalTopSpeaker']
    twitter_frame = pd.read_csv('updated_twitter.csv')
    twitter_frame = twitter_frame[twitter_frame['name'] == assigned_speaker]

    res_fr = pd.DataFrame(quote_item, columns=['articleID', 'quoteID', 'quotation', 'leftContext', 'rightContext', 'globalTopSpeaker', 'date', 'url', 'editDistance', 'editDistanceTweetID', 'lcsDistance', 'lcsDistanceTweetID', 'jaccardDistance', 'jaccardDistanceTweetID'])

    cosine_distances = {}

    for _, row in twitter_frame.iterrows():
        cosine_distances[row['tweet_id']] = 1 - util.cos_sim(row['embedding'], quote_item['embedding'])
    cosine_distances = [1-dist for dist in cosine_distances]
    res_fr['sem_tweet_id'] = min(cosine_distances, key=cosine_distances.get)
    res_fr['cosine_distance'] = min(list(cosine_distances.values()))
    return res_fr


def get_semantic_similarity(quote_data_frame):
    """Returns a frame containing cosine distance to the most similar tweet and the corresponding tweet-id."""
    number_of_quotes = len(quote_data_frame)
    i = 0
    final_results = []
    while i > number_of_quotes:
        data_chunk = quote_data_frame[i:i+5]
        with ProcessPoolExecutor(max_workers=2) as executor:
            result = list(executor.map(compute_semantic_similarity_per_quote, data_chunk, chunksize=7))
            final_results.append(result)
        i = i + 5

    final_results = [item for sublist in final_results for item in sublist]

    return pd.concat(final_results)


def get_data_frame_from_file(csv_file):
    """Reads and returns the dataFrame from the given CSV file (argument)"""
    return pd.read_csv(csv_file)


def get_relevant_authors(speakers_file):
    """Reads the relevant speakers from the file and returns is as a list."""
    speakers = []
    with open(speakers_file, 'r', encoding='latin-1') as fp:
        for speaker in fp:
            speakers.append(speaker)
    speakers = [spk.strip() for spk in speakers]
    return speakers


def update_quotes_frame_with_context(frame):
    """Updates the frame with new column which contains - leftContext+Quotation+rightContext.
    Returns the updated frame."""
    left_context = frame['leftContext'].tolist()
    right_context = frame['rightContext'].tolist()
    quotations = frame['quotation'].tolist()
    left_context = [str(item) for item in left_context]
    right_context = [str(item) for item in right_context]
    quotation = [str(item) for item in quotations]

    combined = []

    for q, l, r in zip(quotation, left_context, right_context):
        l = l.replace("[QUOTE]", "")
        r = r.replace("[QUOTE]", "")
        combined.append(l + " " + q + " " + r)

    frame['combined'] = combined
    return frame


def update_frame_with_embeddings(frame, model, frame_source):
    sent_embeddings = []
    for _, row in frame.iterrows():
        if frame_source == 'twitter':
            sent_embeddings.append(model.encode(row['text'], show_progress_bar=False))
        else:
            sent_embeddings.append(model.encode(row['combined'], show_progress_bar=False))
    frame['embedding'] = sent_embeddings
    return frame


if __name__ == '__main__':
    quote_data_file = 'article_centric_2019.csv'
    twitter_data_file = 'MeTooTweetsWithoutRetweets.csv'
    relevant_speakers_updated_file = 'speaker_updated.txt'

    model = SentenceTransformer('all-MiniLM-L6-v2')

    q_frame = get_data_frame_from_file(quote_data_file)
    rel_speakers = get_relevant_authors(relevant_speakers_updated_file)
    # temp
    q_frame = q_frame[:100]
    # temp
    t_frame = get_data_frame_from_file(twitter_data_file)
    # temp
    t_frame = t_frame[:200]
    # temp
    q_frame = q_frame[q_frame['globalTopSpeakers'].isin(rel_speakers)]
    q_frame = update_quotes_frame_with_context(q_frame)
    q_frame_with_embeddings = update_frame_with_embeddings(q_frame, model, "quotes")

    t_frame_with_embeddings = update_frame_with_embeddings(t_frame, model, "twitter")
    t_frame_with_embeddings.to_csv('updated_twitter.csv')

    final_frame = get_semantic_similarity(q_frame)
    final_frame.to_csv('frame_2019_with_cosine_dist.csv')





