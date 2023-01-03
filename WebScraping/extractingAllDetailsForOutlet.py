from bs4 import BeautifulSoup
import requests
import pandas as pd

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


def get_outlets_data(url, polarity):
    """Url to be scraped and the polarity of the news-outlets in url are the inputs. Returns a frame containing all
    the news outlets in the given url and their polarity. """
    outlets = []
    outlets_url = []

    html_content = requests.get(url).text
    soup = BeautifulSoup(html_content, 'lxml')

    all_td_elements = soup.find_all('td')

    for td in all_td_elements:
        if td.a:
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

    bias_value = [polarity] * len(outlets)

    data = {
        'News_Outlets': outlets,
        'media_bias_root_url': root_url,
        'polarity': bias_value
    }

    frame = pd.DataFrame(data)

    return frame


if __name__ == '__main__':

    frames_lst = []

    for polarity_type in OUTLET_POLARITY_DICT:
        frames_lst.append(get_outlets_data(OUTLET_POLARITY_DICT[polarity_type], polarity_type))

    final_frame = pd.concat(frames_lst)
    final_frame = final_frame.reset_index(drop=True)
    final_frame.to_csv('news_outlets_polarity_all.csv')


