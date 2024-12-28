from typing import Optional

import re
from tqdm import tqdm
import requests
from bs4 import BeautifulSoup
import pickle
import json

test_url = "https://starwars.fandom.com/wiki/Vespaara"

scraped = {}
failed = {}
partition_size = 5000
folder = './Dataset/'
last_number= 0
is_saving_enabled = True

# !rm -rf ./data
# !mkdir -p ./data



def scrape_page_to_soup(page_url: str) ->  Optional[BeautifulSoup]:
    """
    Fetches the HTML content of a webpage and parses it into a BeautifulSoup object.

    Args:
        url (str): The URL of the webpage to scrape.

    Returns:
        Optional[BeautifulSoup]: A BeautifulSoup object if the request is successful, None otherwise.
    """
    try:
        result = requests.get(page_url)
        result.raise_for_status()  # Raises an HTTPError for bad responses
        content = result.content
        return BeautifulSoup(content, "html.parser")
    except requests.RequestException as e:
        print(f"An error occurred while fetching the page: {e}")
        return None




with open('./Dataset/starwars_all_canon_dict.pickle', 'rb') as f:
    pages = pickle.load(f)


# is_saving_enabled = False
# test_pages = {
#     "Vespaara": test_url
# }
# for ix, (key, page_url) in tqdm(enumerate(test_pages.items()), total=(len(test_pages))):


for ix, (key, page_url) in tqdm(enumerate(pages.items()), total=(len(pages))):
    try:
        
        # Get page
        soup = scrape_page_to_soup(page_url)

        # Get title
        heading = soup.find('h1', id='firstHeading')
        if heading is None: 
            print(f"page had no h1 firstHeading {page_url}")
            continue
        heading = heading.text

        # Extract Sidebar
        is_character = False
        side_bar = {}
        side_bar_meta = {'is_character': is_character}
        sec = soup.find_all('section', class_='pi-item')
        for s in sec:
            title = s.find('h2')
            if title is None:
                title = '<no category>'
                m_title = ""
            else:
                title = title.text
                m_title = re.sub(r'\W+', '_', title.lower()).strip('_')
            side_bar[title] = {}
            items = s.find_all('div', class_='pi-item')
            for item in items:
                attr = item.find('h3', class_='pi-data-label')
                if attr is None:
                    attr = '<no attribute>'
                    m_attr = 'no_attribute'
                else:
                    attr = attr.text
                    m_attr = re.sub(r'\W+', '_', attr.lower()).strip('_')
                if attr == 'Species': 
                    is_character = True
                    side_bar_meta['is_character'] = is_character
                # value = re.sub("[\(\[].*?[\)\]]" ,'', '], '.join(item.find('div', class_='pi-data-value').text.split(']')))
                value = re.sub(r"[\(\[].*?[\)\]]", '', '], '.join(item.find('div', class_='pi-data-value').text.split(']')))
                value = value.strip()[:-1].replace(',,', ',')
                if ',' in value:
                    value = [i.strip() for i in value.split(',') if i.strip() != '']
                side_bar[title][attr] = value
                m_key = "_".join([m_title, m_attr])
                side_bar_meta[m_key] = value


        ## Raw page content

        raw_content = soup.find('div', class_='mw-parser-output')
        keywords = []
        lore_pgs = []
        behind_the_scenes_pgs = []
        if raw_content is not None:
            lore_pgs.append(f"# {heading.strip()}")
            write_to_lore = True
            for child in raw_content.find_all(recursive=False):

                ##remove asides
                for aside in child.find_all("aside"):
                    aside.replaceWith('')

                # Handle <h2> tags
                if child.name == 'h2':
                    headline = child.find('span', class_='mw-headline')
                    if headline:
                        appending = f"## {headline.text.strip()}"
                        if appending == "## Behind the scenes":
                            write_to_lore = False
                        if appending in ["## Appearances", "## Sources", "## Notes and references", "## External links"]:
                            continue
                        lore_pgs.append(appending) if write_to_lore  else behind_the_scenes_pgs.append(appending)
                
                # Handle <p> tags
                elif child.name == 'p':
                    cleaned_paragraph = re.sub(r"[\(\[].*?[\)\]]", '', child.text.strip())
                    print(cleaned_paragraph)
                    lore_pgs.append(cleaned_paragraph) if write_to_lore else behind_the_scenes_pgs.append(cleaned_paragraph)

            # Cross-links
            for link in raw_content.find_all('a'):
                part = link.get('href')
                if part is not None:
                    part = part.split('/')[-1] 
                    if part in pages.keys() and part != key:
                        keywords.append(part)
            keywords = list(set(keywords))


        # Data object
        scraped[key] = {
            'id': key,
            'url': page_url,
            'title': heading.strip(),
            'side_bar_json': json.dumps(side_bar),
            'metadata': side_bar_meta,
            'lore': "\n\n".join(lore_pgs),
            'behind_the_scenes': "\n\n".join(behind_the_scenes_pgs),
            'crosslinked_keywords': keywords,
        }

        if 'lore' in scraped[key] and scraped[key]['lore'] == "":
            del scraped[key]['lore']
            
        if 'behind_the_scenes' in scraped[key] and scraped[key]['behind_the_scenes'] == "":
            del scraped[key]['behind_the_scenes']

        # print(json.dumps(scraped[key],indent=4))

        
        # save partition
        if is_saving_enabled:
            if (ix + 1) % partition_size == 0:
                last_number = (ix+1) // partition_size
                fn = folder + f'starwars_all_canon_data_{last_number}.pickle'
                with open(fn, 'wb') as f:
                    pickle.dump(scraped, f, protocol=pickle.HIGHEST_PROTOCOL)
                scraped = {}
    except Exception as e:
        print(f'Failed! {e}')
        failed[key] = page_url
    
    
# Save final part to disk
if is_saving_enabled:
    fn = folder + f'starwars_all_canon_data_{last_number + 1}.pickle'
    with open(fn, 'wb') as f:
        pickle.dump(scraped, f, protocol=pickle.HIGHEST_PROTOCOL)  