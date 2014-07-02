__author__ = 'faye'
import requests
import json
import sys
from bs4 import BeautifulSoup
reload(sys)
sys.setdefaultencoding('utf-8')


def parse(result, timestamp):
    result_soup = BeautifulSoup(result)

    authors = []
    for author in result_soup.find_all("span", class_="person"):
        author = author.getText("|", strip=True)
        author = str(author).replace(',','')
        if "|mail|" or "|mail" in author:
            author = str(author).replace('|mail|','')
            author = str(author).replace('|mail','')
        authors.append(author)

    keywords = []
    for keyword in result_soup.find_all(class_="flagText"):
        keyword = keyword.string
        keywords.append(str(keyword))

    figures = []
    for figure in result_soup.find_all(class_="img"):
        for link in figure.find_all('a'):
            figures.append("http://www.plosone.org" + link.get('href'))

    abstract = result_soup.find(class_="abstract").getText()
    abstract = " ".join(abstract.split())

    payload = {
        "doc":
            json.dumps({
                'title': result_soup.title.string,
                'contributors': authors,
                'properties': {
                    'abstract': abstract,
                    'tags': keywords,
                    'figures': figures,
                    'PDF': result_soup.find("meta", {"name":"citation_pdf_url"})['content']
                },
                'meta': {},
                'id': result_soup.find("meta", {"name":"citation_doi"})['content'],
                'source': "PLoS"
            }),
        'timestamp': timestamp
    }
    return requests.post('http://0.0.0.0:1337/process', params=payload)
