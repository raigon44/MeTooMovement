import pandas as pd
import numpy as np


def clean_up_keywords(frame):

    list_of_keywords = frame['word'].tolist()
    list_of_keywords = [item for item in list_of_keywords if item.isalnum()]
    list_of_keywords = [item for item in list_of_keywords if item not in tokens_to_be_removed]
    list_of_keywords.extend(hashtags_list)

    return list_of_keywords


if __name__ == '__main__':
    tokens_to_be_removed = []
    blacklist_fileName = 'blacklist_Keywords.txt'
    with open(blacklist_fileName) as f:
        lines = f.readlines()
    tokens_to_be_removed = [item.strip('\n') for item in lines]

    df = pd.read_csv('top200MeToo.csv')

    with open('UpdatedKeywordList2.txt') as f:
        lines = f.readlines()
    hashtags_list = [item.strip('\n').lower() for item in lines]

    clean_keywords = clean_up_keywords(df)

    print(clean_keywords)

    print(len(clean_keywords))



