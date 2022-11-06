import time
import multiprocessing
import pandas as pd
import json
import concurrent.futures


def load_data_frame(data_file):
    return pd.read_csv(data_file)


def get_author_names(frame_1):
    return frame_1['speaker'].value_counts().keys().tolist()


def extract_fields_per_article(row):

    data = json.loads(row)
    spk = []
    with open("speaker.txt", "r") as f:

        for line in f:
            spk.append(line.strip())
    f_res = []
    for item in data['quotations']:
        res = []
        if item['globalTopSpeaker'] in spk:

            res.append(data['articleID'])
            res.append(item['quoteID'])
            res.append(item['quotation'])
            res.append(item['leftContext'])
            res.append(item['rightContext'])
            res.append(item['globalTopSpeaker'])
            res.append(data['date'])
            res.append(data['url'])
            f_res.append(res)

    if f_res:
        return f_res
    else:
        return


def construct_frame(article_centric_json_file):

    json_lines = [line for line in open(article_centric_json_file, 'r', encoding='latin-1')]

    i = 0
    #j = len(json_lines)
    j = 200

    results_1 = []

    while i < j:
        temp_json_lines = json_lines[i:i + 6]
        #print(temp_json_lines)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = executor.map(extract_fields_per_article, temp_json_lines)
        i = i + 6
        results_1.append(results)

    return results_1


if __name__ == '__main__':
    start = time.perf_counter()

    final_result = construct_frame('1.json')

    #print(type(final_result))

    final_rows = []
    for item in final_result:
        for entry in item:
            if entry is not None:
                for rw in entry:
                    final_rows.append(rw)

    print(final_rows)
    #final_result = [item for item in final_result if item is not None]
    #print(final_result)
    # if final_rows:
    #     final_rows = final_rows[0]

    article_frame = pd.DataFrame(final_rows, columns=['articleID', 'quoteID', 'quotation', 'leftContext', 'rightContext',
                                          'globalTopSpeaker', 'date', 'url'])

    article_frame.to_csv("article.csv")
    finish_time = time.perf_counter()
    print(f'Finished in {round(finish_time-start, 2)} second(s)')
