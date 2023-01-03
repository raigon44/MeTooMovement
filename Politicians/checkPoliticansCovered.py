import json
import pandas as pd


def get_names_of_politicians_from_bio_guide(json_file):

    politician_names = []
    with open(json_file, 'r', encoding='utf-8') as fp:
        json_data = json.load(fp)

    for politician_info in json_data:
        #politician_names.append(str.lower(politician_info['givenName']+' '+politician_info['familyName']))
        politician_names.append(politician_info['id'])

    return politician_names


def get_names_of_politicians_from_wikidata(json_file):
    json_data = []
    with open(json_file, 'r', encoding='utf-8') as fp:
        for line in fp:
            json_data.append(json.loads(line))

    df = pd.json_normalize(json_data)
    df_sub = df[~df['US_congress_bio_ID'].isnull()]

    return df_sub['US_congress_bio_ID'].tolist()
    #
    # politician_names = [str.lower(politician.get('label')) for politician in json_data]
    #
    #
    #
    # return politician_names


def get_politicians_coverage(wiki_data_dump, us_congress_bio_guide):

    politicians_list_from_bio_guide = get_names_of_politicians_from_bio_guide(us_congress_bio_guide)
    politicians_list_from_wiki_data = get_names_of_politicians_from_wikidata(wiki_data_dump)
    count_politician_from_bio_guide = len(politicians_list_from_bio_guide)

    print(len(politicians_list_from_bio_guide))
    print(len(politicians_list_from_wiki_data))
    print(len(list(set(politicians_list_from_bio_guide) & set(politicians_list_from_wiki_data))))

    #return len(list(set(politicians_list_from_wiki_data).difference(set(politicians_list_from_bio_guide))))

    return (len(list(set(politicians_list_from_bio_guide) & set(politicians_list_from_wiki_data))) /
            count_politician_from_bio_guide) * 100


if __name__ == '__main__':
    coverage = get_politicians_coverage(r'C:\Users\raigo\PycharmProjects\MeTooMovement\WikidataDump\output.json',
                                        'politiciansFromBioGuide.json')
    print(coverage)


