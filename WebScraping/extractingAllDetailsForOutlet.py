from bs4 import BeautifulSoup
import bs4
import requests
import pandas as pd
import re

OUTLET_POLARITY_DICT = {
    'center': 'https://mediabiasfactcheck.com/center/',
    'right': 'https://mediabiasfactcheck.com/right/',
    'left': 'https://mediabiasfactcheck.com/left/',
    'left_center': 'https://mediabiasfactcheck.com/leftcenter/',
    'right_center': 'https://mediabiasfactcheck.com/right-center/',
    'conspiracy': 'https://mediabiasfactcheck.com/conspiracy/',
    'questionable': 'https://mediabiasfactcheck.com/fake-news/',
    'pro_science': 'https://mediabiasfactcheck.com/pro-science/',
    'satire': 'https://mediabiasfactcheck.com/satire/'
}


def get_factual_reporting_value_for_outlet(p_element_text_lst: list) -> str:

    #try:
        #return re.search('Factual Reporting: (.*)', p_element.text).group(1)
    for item in p_element_text_lst:
        if item.startswith('Factual Reporting:'):
            return item.split(':')[1]

    # except IndexError as ie:
    #
    #     print(ie)
    #
    # except AttributeError as ae:
    #
    #     print(ae)


def get_country_for_outlet(p_element_text_lst: list) -> str:
    for item in p_element_text_lst:
        if item.startswith('Country:'):
            return item.split(':')[1]
    return False

    #return re.search('Country: (.*)', p_element.text).group(1)


def get_press_freedom_for_outlet(p_element_text_lst: list) -> str:
    for item in p_element_text_lst:
        if item.startswith('Press Freedom Rating:'):
            return item.split(':')[1]
    return False
    #return re.search('Press Freedom Rating: (.*)', p_element.text).group(1)


def get_media_type_for_outlet(p_element_text_lst: list) -> str:
    for item in p_element_text_lst:
        if item.startswith('Media Type:'):
            return item.split(':')[1]
    return False
    #return re.search('Media Type: (.*)', p_element.text).group(1)


def get_traffic_for_outlet(p_element_text_lst: list) -> str:
    for item in p_element_text_lst:
        if item.startswith('Traffic/Popularity:'):
            return item.split(':')[1]
    return False
    #return re.search('Traffic/Popularity: (.*)', p_element.text).group(1)


def get_credibility_for_outlet(p_element_text_lst: list) -> str:
    for item in p_element_text_lst:
        if item.startswith('MBFC Credibility Rating:'):
            return item.split(':')[1]
    return False
    #return re.search('MBFC Credibility Rating: (.*)', p_element.text).group(1)


def get_detailed_report_for_all_outlets(news_outlets_urls: list):

    factual_reporting, country, press_freedom, media_type, traffic, credibility = [], [], [], [], [], []

    for url in news_outlets_urls:

        html_content = requests.get(url).text
        soup = BeautifulSoup(html_content, 'lxml')
        # h_element = soup.find('h3', text='Detailed Report')
        # p_element = h_element.find_next_siblings()

        paragraph_elements = soup.find_all('p')
        for elem in paragraph_elements:
            if elem.text.startswith('Bias Rating: '):
                p_element = elem
                break

        p_element_text = p_element.text.split('\n')

        factual_reporting.append(get_factual_reporting_value_for_outlet(p_element_text))
        country.append(get_country_for_outlet(p_element_text))
        press_freedom.append(get_press_freedom_for_outlet(p_element_text))
        media_type.append(get_media_type_for_outlet(p_element_text))
        traffic.append(get_traffic_for_outlet(p_element_text))
        credibility.append(get_credibility_for_outlet(p_element_text))

    return factual_reporting, country, press_freedom, media_type, traffic, credibility


def get_news_outlets(url, polarity):
    """Url to be scraped and the polarity of the news-outlets in url are the inputs. Returns a frame containing all
    the news outlets in the given url and their polarity. """
    outlets = []
    outlets_url = []

    html_content = requests.get(url).text
    soup = BeautifulSoup(html_content, 'lxml')

    all_td_elements = soup.find_all('td')

    for td in all_td_elements:
        if td.a:
            outlets_url.append(td.a.get('href'))
            outlets.append(td.text)

    for td in all_td_elements:
        if td.a:
            outlets_url.append(td.a)

    root_url = []
    for item in outlets:
        if len(item.split()) > 1:
            root_url.append(item.split()[-1])
        else:
            root_url.append(item.split()[0])

    root_url = [item.lstrip('(') for item in root_url]
    root_url = [item.rstrip(')') for item in root_url]

    # Here I will call the function and pass the list of urls to the outlets

    factual_reporting, country, press_freedom, media_type, traffic, credibility = \
        get_detailed_report_for_all_outlets(outlets_url)

    bias_value = [polarity] * len(outlets)

    data = {
        'News_Outlets': outlets,
        'media_bias_root_url': root_url,
        'polarity': bias_value,
        'Factual_reporting' : factual_reporting,
        'Country': country,
        'press_freedom': press_freedom,
        'media_type': media_type,
        'traffic': traffic,
        'credibility': credibility
    }

    frame = pd.DataFrame(data)

    return frame


if __name__ == '__main__':

    frames_lst = []

    for polarity_type in OUTLET_POLARITY_DICT:
        frames_lst.append(get_news_outlets(OUTLET_POLARITY_DICT[polarity_type], polarity_type))

    final_frame = pd.concat(frames_lst)
    final_frame = final_frame.reset_index(drop=True)
    final_frame.to_csv('news_outlets_polarity_all.csv')


