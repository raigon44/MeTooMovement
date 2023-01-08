import pandas
import pandas as pd
import gzip
import os
import urllib

IMDB_NAMES_DATA_DOWNLOAD_URL = 'https://datasets.imdbws.com/name.basics.tsv.gz'
IMDB_NAMES_DATA_FILE = 'name.basics.tsv.gz'
CELEBRITY_TWITTER_USER_DATA = 'twitter_authors.csv'


def read_imdb_data_file(data_file: str) -> pandas.DataFrame:

    with gzip.open(data_file) as fp:
        imdb_names_frame = pd.read_csv(fp)
        return imdb_names_frame


def get_the_relevant_celebrity_names(celeb_data_file: str) -> list:
    frame = pd.read_csv(celeb_data_file)
    return frame['author_name'].tolist()


def get_IMDB_user_data_frame(data_file: str) -> pandas.DataFrame:

    if os.path.exists(data_file):
        print("IMDB names data file already present in the directory!!!")
        return read_imdb_data_file(data_file)

    else:

        print("IMDB names data file not present in the directory!!!!")
        print("Trying to download....")

        urllib.urlretrieve(IMDB_NAMES_DATA_DOWNLOAD_URL, './')

        if os.path.exists('./name.basics.tsv.gz'):
            print("Download successful!!!")
            return read_imdb_data_file(data_file)
        else:
            print("Download failed!!! Please run the script after manually downloading the data file!!!!")
            exit(0)


def get_imdb_unique_ids(imdb_data_file: str, celeb_names: list) -> list:

    imdb_frame = get_IMDB_user_data_frame(imdb_data_file)

    return imdb_frame[imdb_frame['primaryName'].isin(celeb_names)]['nconst'].tolist()


if __name__ == '__main__':
    list_of_celeb_names = get_the_relevant_celebrity_names(CELEBRITY_TWITTER_USER_DATA)

    get_imdb_unique_ids(IMDB_NAMES_DATA_FILE, )





