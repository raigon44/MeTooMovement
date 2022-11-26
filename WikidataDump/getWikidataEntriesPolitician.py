from argparse import ArgumentParser
import time
import bz2
import csv
import io
import json

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

parser = ArgumentParser()
parser.add_argument("--input", required=True)
parser.add_argument("--output", required=True)
parser.add_argument("--occupations", required=True)
args = parser.parse_args()


def process_func(s):
    s["label"] = s["labels"]["en"]["value"]
    if s.get('labels') is not None:
        del s['labels']

    # Occupation
    if len(s.get("claims", {}).get("P106", [])) > 0:
        tmp = []
        for v in s["claims"]["P106"]:
            # id: "Q123", "numeric-id": 123
            if (
                    v["mainsnak"].get("datavalue", {}).get("value", {}).get("id")
                    is not None
            ):
                tmp.append(v["mainsnak"]["datavalue"]["value"]["id"])
        if len(tmp) > 0:
            s["occupation"] = tmp

    # Gender
    if len(s.get("claims", {}).get("P21", [])) > 0:
        tmp = []
        for v in s["claims"]["P21"]:
            # id: "Q123", "numeric-id": 123
            if (
                    v["mainsnak"].get("datavalue", {}).get("value", {}).get("id")
                    is not None
            ):
                tmp.append(v["mainsnak"]["datavalue"]["value"]["id"])
        if len(tmp) > 0:
            s["gender"] = tmp

    # Country of citizenship
    if len(s.get("claims", {}).get("P27", [])) > 0:
        tmp = []
        for v in s["claims"]["P27"]:
            # id: "Q123", "numeric-id": 123
            if (
                    v["mainsnak"].get("datavalue", {}).get("value", {}).get("id")
                    is not None
            ):
                tmp.append(v["mainsnak"]["datavalue"]["value"]["id"])
            if len(tmp) > 0:
                s["nationality"] = tmp

    # Position Held
    if len(s.get("claims", {}).get("P39", [])) > 0:
        tmp = []
        for v in s["claims"]["P39"]:
            # id: "Q123", "numeric-id": 123
            if (
                    v["mainsnak"].get("datavalue", {}).get("value", {}).get("id")
                    is not None
            ):
                tmp.append(v["mainsnak"]["datavalue"]["value"]["id"])
            if len(tmp) > 0:
                s["positions_held"] = tmp

    # Date of Birth
    if len(s.get("claims", {}).get("P569", [])) > 0:
        tmp = []
        for v in s["claims"]["P569"]:
            if (
                    v["mainsnak"].get("datavalue", {}).get("value", {}).get("time")
                    is not None
            ):
                tmp.append(v["mainsnak"]["datavalue"]["value"]["time"])
            if len(tmp) > 0:
                s["date_of_birth"] = tmp

    # Academic Degree
    if len(s.get("claims", {}).get("P512", [])) > 0:
        tmp = []
        for v in s["claims"]["P512"]:
            if (
                    v["mainsnak"].get("datavalue", {}).get("value", {}).get("id")
                    is not None
            ):
                tmp.append(v["mainsnak"]["datavalue"]["value"]["id"])
            if len(tmp) > 0:
                s["academic_degree"] = tmp

    # Member of Political Party
    if len(s.get("claims", {}).get("P102", [])) > 0:
        tmp = []
        for v in s["claims"]["P102"]:
            if (
                    v["mainsnak"].get("datavalue", {}).get("value", {}).get("id")
                    is not None
            ):
                tmp.append(v["mainsnak"]["datavalue"]["value"]["id"])
            if len(tmp) > 0:
                s["party"] = tmp

    # Candidacy in election
    if len(s.get("claims", {}).get("P3602", [])) > 0:
        tmp = []
        for v in s["claims"]["P3602"]:
            if (
                    v["mainsnak"].get("datavalue", {}).get("value", {}).get("id")
                    is not None
            ):
                tmp.append(v["mainsnak"]["datavalue"]["value"]["id"])
            if len(tmp) > 0:
                s["candidacy"] = tmp

    # US Congress Bio ID
    # Get more information on the politicians based on the ID here: https://bioguide.congress.gov
    if len(s.get("claims", {}).get("P1157", [])) > 0:
        tmp = None
        for v in s["claims"]["P1157"]:
            if (
                    v["mainsnak"].get("datavalue", {}).get("value")
                    is not None
            ):
                tmp = v["mainsnak"]["datavalue"]["value"]
                break
        if tmp is not None:
            s["US_congress_bio_ID"] = tmp

    # Ethnic Group
    if len(s.get("claims", {}).get("P172", [])) > 0:
        tmp = []
        for v in s["claims"]["P172"]:
            if (
                    v["mainsnak"].get("datavalue", {}).get("value", {}).get("id")
                    is not None
            ):
                tmp.append(v["mainsnak"]["datavalue"]["value"]["id"])
            if len(tmp) > 0:
                s["ethnic_group"] = tmp

    # Religion
    if len(s.get("claims", {}).get("P140", [])) > 0:
        tmp = []
        for v in s["claims"]["P140"]:
            if (
                    v["mainsnak"].get("datavalue", {}).get("value", {}).get("id")
                    is not None
            ):
                tmp.append(v["mainsnak"]["datavalue"]["value"]["id"])
            if len(tmp) > 0:
                s["religion"] = tmp

    # Twitter User account
    if len(s.get("claims", {}).get("P8687", [])) > 0:
        tmp = []
        for v in s["claims"]["P8687"]:
            if "P6552" in v["qualifiers"]:
                try:

                    tmp.append(v["qualifiers"]["P6552"][0]["datavalue"]["value"])
                except KeyError:
                    print(s)

            if len(tmp) > 0:
                s["twitter_id"] = tmp

    # Aliases. Removing leftovers and unnecessary attributes
    if len(s.get("aliases", {}).get("en", [])) > 0:
        s["aliases"] = [v["value"] for v in s["aliases"]["en"]]
    elif s.get("aliases") is not None:
        del s["aliases"]
    if s.get("descriptions") is not None:
        del s["descriptions"]
    if s.get("sitelinks") is not None:
        del s["sitelinks"]
    if s.get("claims") is not None:
        del s["claims"]

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


def is_criteria_satisfied(json_data, jobids):
    """
    This function check if the given wikidata entry satisfies all the required conditions.
    :param json_data: a single entry in Wikidata dump. jobids: a list of relevant job-ids
    :return: True/False
    """

    # Conditions to be checked:
    # 0. Type of entry item or property
    # 1. Instance of human
    # 2. English label
    # 3. Is the occupation related to Politics
    # 4. Does the given politician have a twitter account

    if json_data['type'] != "item":
        return False
    if json_data.get("labels", {}).get("en") is None:
        return False
    if json_data.get("claims", {}).get("P31", []) is None:
        return False
    if len(json_data.get("claims", {}).get("P31", [])) > 0:
        for entry in json_data["claims"]["P31"]:
            flag = 0
            if (entry['mainsnak'].get('datavalue', {}).get('value', {}).get('id') is not None):
                if entry['mainsnak']['datavalue']['value']['id'] == 'Q5':
                    flag = 1
                    break
        if flag == 0:
            return False
    if json_data.get("claims", {}).get("P106") is None:
        return False
    if len(json_data.get("claims", {}).get("P106")) > 0:

        for entry in json_data["claims"]["P106"]:
            flag = 0
            if (entry['mainsnak'].get('datavalue', {}).get('value', {}).get('id') is not None):
                if entry['mainsnak']['datavalue']['value']['id'] in jobids:
                    flag = 1
                    break
        if flag == 0:
            return False
    if len(json_data.get("claims", {}).get("P8687", [])) == 0:
        return False

    for entry in json_data["claims"]["P8687"]:
        flag = 0
        if "P6552" in entry["qualifiers"]:
            flag = 1
            break
    if flag == 0:
        return False

    return True


def process_wikidata_dump(
        data_file,  # Wikidata JSON dump file
        stop_after_row_count=0,  # stops execution after number of rows is processed, used for testing
        skip=0,  # start processing from specified row index. Could be used to resume process
        process_list_max_size=50000,  # how many rows processed in batch
        max_workers=12,  # number of parallel processing threads
        buffer_size=1024 * 1024):  # dump file read cache size in bytes

    process_start = time.time()
    fails = 0
    start = time.time()
    political_occupations = json.load(open(args.occupations, 'r'))
    jobIDs = list(political_occupations.keys())
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
                if not is_criteria_satisfied(s, jobIDs):
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
