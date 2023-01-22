from string import ascii_letters
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from argparse import ArgumentParser
from matplotlib.colors import ListedColormap

parser = ArgumentParser()
parser.add_argument("--input", required=True)
args = parser.parse_args()


def load_frame(data_file):
    return pd.read_csv(data_file)


def preprocess_frame(frame):
    frame.drop(['Unnamed: 0', 'tweet_id', 'text', 'reported'], axis=1, inplace=True)
    return frame


def correlation_heatmap_plot_half_and_half(correlation_matrix, frame_columns):
    sns.set_theme(style="white")
    mask = np.triu(np.ones_like(correlation_matrix, dtype=bool))
    f, ax = plt.subplots(figsize=(11, 9))
    cmap = sns.diverging_palette(230, 20, as_cmap=True)
    ax = sns.heatmap(correlation_matrix, cmap=cmap, center=0,
                     square=True, linewidths=.5, cbar_kws={"shrink": .5})
    mask = np.ones_like(correlation_matrix) - mask

    ax = sns.heatmap(correlation_matrix, mask=mask, cmap=ListedColormap(['white']), annot=True, cbar=False, linewidths=1.5,
                         linecolor='black', xticklabels=frame_columns, yticklabels=frame_columns)

    plt.show()

    return


def correlation_heatmap_plot_only_half(correlation_matrix, frame_columns):
    pass


if __name__ == '__main__':

    data_frame = load_frame(args.input)

    frame_reported = data_frame[data_frame['reported'] == 1]
    frame_not_reported = data_frame[data_frame['reported'] != 1]

    # All frame
    all_frame = preprocess_frame(data_frame)
    corr_matrix = data_frame.corr()

    correlation_heatmap_plot_half_and_half(corr_matrix, data_frame.columns)

    # Reported
    frame_reported = preprocess_frame(frame_reported)
    corr_matrix = frame_reported.corr()

    correlation_heatmap_plot_half_and_half(corr_matrix, frame_reported.columns)

    #Not reported
    frame_not_reported = preprocess_frame(frame_not_reported)
    corr_matrix = frame_not_reported.corr()

    correlation_heatmap_plot_half_and_half(corr_matrix, frame_not_reported.columns)




