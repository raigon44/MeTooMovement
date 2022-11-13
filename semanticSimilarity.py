import pandas as pd

def compute_semantic_similarity(model, quote_data_frame, twitter_data_frame, speakers):



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


if __name__ == '__main__':
    quote_data_file = 'article_centric_2019.csv'
    twitter_data_file = 'MeTooTweetsWithoutRetweets.csv'
    relevant_speakers_updated_file = 'speaker_updated.txt'

