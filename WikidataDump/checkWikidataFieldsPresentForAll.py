import pandas as pd
from argparse import ArgumentParser
import time
import bz2
import csv
import io
import json

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor


"""This program recieves as input a list of QID's and a list of wikidata attributes to be checked
    For the given QID, we will check if the user have the needed field and will extract it"""
"""I think the easiest thing to do will be to get the dump and then write another function which can get/check for the
needed attributes."""

#Step 1: Get the QID of the hollywood users in Wikidata - done
#Step 2: Get the id's of the relevant attributes from Wikidata
#Step 3: Have a check if the data is an item and not a property
#Step 4: Save the details

parser = ArgumentParser()
parser.add_argument("--input", required=True)
parser.add_argument("--output", required=True)
parser.add_argument("--wikidata", required=True)
args = parser.parse_args()


def get_relevant_qids():
    frame = pd.read_csv(args.wikidata)
    qids = frame['clean-qid'].tolist()
    return list(set(qids))


def process_func(s):
    s["label"] = s["labels"]["en"]["value"]
    if s.get('labels') is not None:
        del s['labels']
    if s.get("descriptions") is not None:
        del s["descriptions"]
    if s.get("sitelinks") is not None:
        del s["sitelinks"]
    if len(s.get("aliases", {}).get("en", [])) > 0:
        s["aliases"] = [v["value"] for v in s["aliases"]["en"]]
    elif s.get("aliases") is not None:
        del s["aliases"]
    return s


# run JSON processing in parallel threads
def process_list_executor(process_func, process_list, output_file, max_workers):
    row_count = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        result = executor.map(process_func, process_list)
    for json_data in result:
        if json_data is not None:
            row_count += 1
            output_file.write(json.dumps(json_data, ensure_ascii=False) + "\n")
    return row_count


def is_criteria_satisfied(json_data, q_ids):
    """
    This function check if the given wikidata entry satisfies all the required conditions.
    :param json_data: a single entry in Wikidata dump. jobids: a list of relevant job-ids
    :return: True/False
    """

    # Conditions to be checked:
    # 0. Type of entry item or property
    # 1. Instance of human
    # 2. Does the ID present in the list of relevant ID's

    if json_data['type'] != "item":
        return False
    if json_data.get("labels", {}).get("en") is None:
        return False
    if json_data['id'] not in q_ids:
        return False

    return True


def process_wikidata_dump(
        data_file,  # Wikidata JSON dump file
        stop_after_row_count=0,  # stops execution after number of rows is processed, used for testing
        skip=0,  # start processing from specified row index. Could be used to resume process
        process_list_max_size=5000,  # how many rows processed in batch
        max_workers=12,  # number of parallel processing threads
        buffer_size=1024 * 1024):  # dump file read cache size in bytes

    process_start = time.time()
    fails = 0
    start = time.time()

    q_ids = get_relevant_qids()

    with open(args.output, 'w') as output_file:
        with bz2.open(data_file, "r") as bzinput:

            process_list = []
            result_count = 0
            for i, line in enumerate(io.BufferedReader(bzinput, buffer_size=buffer_size)):
                line = line.decode("utf-8")
                try:
                    s = json.loads(line.strip(",\n"))
                except json.JSONDecodeError:
                    fails += 1
                    continue

                if i == 0:
                    continue

                if 0 < stop_after_row_count < i:
                    break

                # display progress after each 10000 rows processed
                if i % 1000000 == 0:
                    end = time.time()
                    print(
                        f">>> idx: {i}, result_count: {result_count}, process_list: {len(process_list)}, time: {end - start}")
                    start = time.time()

                if i < skip:
                    continue

                # skip processing if string pattern is not found
                if not is_criteria_satisfied(s, q_ids):
                    continue

                process_list.append(s)

                if len(process_list) == process_list_max_size:
                    result_count += process_list_executor(process_func, process_list, output_file, max_workers)
                    process_list = []

            result_count += process_list_executor(process_func, process_list, output_file, max_workers)

    process_end = time.time()
    print(f"DONE. {result_count} rows in {process_end - process_start}")

    print(f"Could not read {fails} lines.")

    return


if __name__ == "__main__":

    process_wikidata_dump(args.input)
